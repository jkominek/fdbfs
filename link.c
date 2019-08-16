#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>

#include "util.h"
#include "inflight.h"

/*************************************************************
 * link
 *************************************************************
 * INITIAL PLAN
 * check that the provided inode isn't a directory.
 * check that the provided destination is a directory.
 * construct a dirent and write it into the correct spot.
 *
 * REAL PLAN
 * ??
 *
 * TRANSACTIONAL BEHAVIOR
 * nothing special
 */
struct fdbfs_inflight_link {
  struct fdbfs_inflight_base base;

  fuse_ino_t ino;
  fuse_ino_t newparent;
  char *name;
  int namelen;

  struct stat inoattr;

  // is the file to link a non-directory?
  FDBFuture *file_lookup;
  // is the destination location a directory?
  FDBFuture *dir_lookup;
  // does the destination location already exist?
  FDBFuture *target_lookup;
};

void fdbfs_link_commit_cb(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_link *inflight = p;
  
  struct fuse_entry_param e;
  bzero(&e, sizeof(struct fuse_entry_param));
  e.ino = inflight->ino;
  e.generation = 1;
  bcopy(&(inflight->inoattr), &(e.attr), sizeof(struct stat));
  e.attr_timeout = 0.01;
  e.entry_timeout = 0.01;

  fuse_reply_entry(inflight->base.req, &e);
  fdb_future_destroy(f);
  fdbfs_inflight_cleanup(p);
}

void fdbfs_link_check(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_link *inflight = p;

  if(!fdb_future_is_ready(inflight->dir_lookup)) {
    fdb_future_set_callback(inflight->dir_lookup, fdbfs_error_checker, p);
    return;
  }
  if(!fdb_future_is_ready(inflight->target_lookup)) {
    fdb_future_set_callback(inflight->target_lookup, fdbfs_error_checker, p);
    return;
  }

  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;

  int err = 0; // fuse error, if there is one
  
  // is the file a non-directory?
  fdb_future_get_value(inflight->file_lookup, &present,
		       (const uint8_t **)&val, &vallen);
  if(present) {
    unpack_stat_from_dbvalue(val, vallen, &(inflight->inoattr));
    if((inflight->inoattr.st_mode & S_IFMT) == S_IFDIR) {
      // can hardlink anything except a directory
      err = EPERM;
    }
    // we could lift this value and save it for the
    // other dirent we need to create?
  } else {
    // apparently it isn't there. sad.
    err = ENOENT;
  }    

  // is the directory a directory?
  fdb_future_get_value(inflight->dir_lookup, &present,
		       (const uint8_t **)&val, &vallen);
  if(present) {
    struct stat dirattr;
    unpack_stat_from_dbvalue(val, vallen, &dirattr);
    if((dirattr.st_mode & S_IFMT) != S_IFDIR) {
      // have to hardlink into a directory
      err = ENOTDIR;
    }
  } else {
    err = ENOENT;
  }

  // Does the target exist?
  fdb_future_get_value(inflight->target_lookup, &present,
		       (const uint8_t **)&val, &vallen);
  if(present) {
    // that's an error. :(
    err = EEXIST;
  }

  // (we'll clean these up now, since we're done with them)
  fdb_future_destroy(inflight->file_lookup);
  fdb_future_destroy(inflight->dir_lookup);
  fdb_future_destroy(inflight->target_lookup);

  if(err) {
    // some sort of error.
    fuse_reply_err(inflight->base.req, err);
    fdbfs_inflight_cleanup(p);
  }

  uint8_t key[1024]; // TODO size
  int keylen;

  // need to update the inode attributes
  pack_inode_key(inflight->ino, key, &keylen);
  inflight->inoattr.st_nlink += 1;
  fdb_transaction_set(inflight->base.transaction,
		      key, keylen,
		      (const uint8_t *)&(inflight->inoattr),
		      sizeof(struct stat));
  // also need to add the new directory entry
  struct dirent direntval;
  bzero(&direntval, sizeof(struct dirent));
  direntval.ino = inflight->ino;
  direntval.st_mode = inflight->inoattr.st_mode & S_IFMT;
  pack_dentry_key(inflight->newparent,
		  inflight->name, inflight->namelen,
		  key, &keylen);
  fdb_transaction_set(inflight->base.transaction,
		      key, keylen,
		      (const uint8_t *)&direntval,
		      sizeof(struct dirent));

  // commit
  fdb_future_set_callback(fdb_transaction_commit(inflight->base.transaction),
			  fdbfs_link_commit_cb, p);
  return;

}

void fdbfs_link_issuer(void *p)
{
  struct fdbfs_inflight_link *inflight = p;

  // TODO ensure this is sized right.
  uint8_t key[1024];
  int keylen;
  // check that the file is a file
  pack_inode_key(inflight->ino, key, &keylen);
  inflight->file_lookup =
    fdb_transaction_get(inflight->base.transaction, key, keylen, 0);

  // check destination is a directory
  pack_inode_key(inflight->newparent, key, &keylen);
  inflight->dir_lookup =
    fdb_transaction_get(inflight->base.transaction, key, keylen, 0);

  // check nothing exists in the destination
  pack_dentry_key(inflight->newparent, inflight->name, inflight->namelen,
		  key, &keylen);
  inflight->target_lookup =
    fdb_transaction_get(inflight->base.transaction, key, keylen, 0);
  
  inflight->base.cb = fdbfs_link_check;
  fdb_future_set_callback(inflight->file_lookup, fdbfs_error_checker, p);
}

void fdbfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname)
{
  int namelen = strlen(newname);

  struct fdbfs_inflight_link *inflight;
  // to just make one allocation, we'll stuff our copy of the name
  // right after the struct.
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_link) + namelen + 1,
				   req,
				   fdbfs_link_check,
				   fdbfs_link_issuer,
				   T_READWRITE);

  inflight->ino = ino;
  inflight->newparent = newparent;
  inflight->namelen = namelen;
  inflight->name = ((char*)inflight) + sizeof(struct fdbfs_inflight_link);

  strncpy(inflight->name, newname, namelen+1);
  
  fdbfs_link_issuer(inflight);
}
