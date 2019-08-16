
#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 600
#include <foundationdb/fdb_c.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

#include "util.h"
#include "inflight.h"

/*************************************************************
 * mknod
 *************************************************************
 * INITIAL PLAN
 * make up a random inode number. check to see if it is allocated.
 * check to see if there is an existing dirent for the name in
 * question. if they're both empty, proceed to make the two
 * records.
 *
 * REAL PLAN
 * ???
 */
struct fdbfs_inflight_mknod {
  struct fdbfs_inflight_base base;
  FDBFuture *inode_check;
  FDBFuture *dirent_check;
  fuse_ino_t parent;
  fuse_ino_t ino;
  struct stat attr;
  char *name;
  int namelen;
  mode_t mode;
  dev_t rdev;
};

void fdbfs_mknod_commit_cb(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_mknod *inflight = p;
  
  struct fuse_entry_param e;
  bzero(&e, sizeof(struct fuse_entry_param));
  e.ino = inflight->ino;
  e.generation = 1;
  bcopy(&(inflight->attr), &(e.attr), sizeof(struct stat));
  e.attr_timeout = 0.01;
  e.entry_timeout = 0.01;

  debug_print("fdbfs_mknod_commit_cb returning ino %lx ino %lx\n", e.ino, e.attr.st_ino);
  
  int ret = fuse_reply_entry(inflight->base.req, &e);
  debug_print("fdbfs_mknod_commit_cb fuse_reply_entry returned %i\n", ret);
  fdb_future_destroy(f);
  fdbfs_inflight_cleanup(p);
}

void fdbfs_mknod_issueverification(void *p);

void fdbfs_mknod_postverification(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_mknod *inflight = p;

  // make sure all of our futures are ready
  if(!fdb_future_is_ready(inflight->inode_check)) {
    fdb_future_set_callback(inflight->inode_check, fdbfs_error_checker, p);
    return;
  }
  if(!fdb_future_is_ready(inflight->dirent_check)) {
    fdb_future_set_callback(inflight->dirent_check, fdbfs_error_checker, p);
    return;
  }

  fdb_bool_t inode_present, dirent_present;
  const uint8_t *value; int valuelen;
  fdb_future_get_value(inflight->dirent_check, &dirent_present, &value, &valuelen);
  fdb_future_get_value(inflight->inode_check, &inode_present, &value, &valuelen);
  
  fdb_future_destroy(inflight->inode_check);
  fdb_future_destroy(inflight->dirent_check);

  if(dirent_present) {
    // can't make this entry, there's already something there
    fuse_reply_err(inflight->base.req, EEXIST);
    fdbfs_inflight_cleanup(p);
    return;
  }

  if(inode_present) {
    // astonishingly we guessed an inode that already exists.
    // try this again!
    fdbfs_mknod_issueverification(p);
    return;
  }

  bzero(&(inflight->attr), sizeof(struct stat));
  // perform the necessary sets here
  inflight->attr.st_dev = 0;
  inflight->attr.st_mode = inflight->mode;
  inflight->attr.st_ino = inflight->ino;
  inflight->attr.st_nlink = (inflight->mode & S_IFDIR) ? 2 : 1;
  inflight->attr.st_uid = 0;
  inflight->attr.st_gid = 0;
  inflight->attr.st_rdev = inflight->rdev;
  inflight->attr.st_size = 0;
  inflight->attr.st_blksize = BLOCKSIZE;
  inflight->attr.st_blocks = 1;
  // set the inode KV pair
  inflight->attr.st_atime = 0;
  inflight->attr.st_ctime = 0;
  inflight->attr.st_mtime = 0;

  uint8_t key[2048]; int keylen;
  
  pack_inode_key(inflight->ino, key, &keylen);
  fdb_transaction_set(inflight->base.transaction,
		      key, keylen,
		      (uint8_t *)&(inflight->attr),
		      sizeof(struct stat));

  struct dirent direntval;
  bzero(&direntval, sizeof(struct dirent));
  direntval.ino = inflight->ino;
  direntval.st_mode = inflight->mode & S_IFMT;

  pack_dentry_key(inflight->parent, inflight->name, inflight->namelen, key, &keylen);
  fdb_transaction_set(inflight->base.transaction,
		      key, keylen,
		      (uint8_t *)&direntval,
		      sizeof(struct dirent));  

  // if the commit works, we can reply to fuse and clean up
  // if it doesn't, the issuer will try again.
  inflight->base.cb = fdbfs_mknod_commit_cb;
  fdb_future_set_callback(fdb_transaction_commit(inflight->base.transaction),
			  fdbfs_error_checker, p);
}

void fdbfs_mknod_issueverification(void *p)
{
  struct fdbfs_inflight_mknod *inflight = p;

  inflight->ino = generate_inode();
  // reset this in case commit failed
  inflight->base.cb = fdbfs_mknod_postverification;

  // pack the inode key
  uint8_t key[512];
  int keylen;

  pack_dentry_key(inflight->parent, inflight->name, inflight->namelen, key, &keylen);
  inflight->dirent_check = fdb_transaction_get(inflight->base.transaction, key, keylen, 0);

  pack_inode_key(inflight->ino, key, &keylen);
  inflight->inode_check = fdb_transaction_get(inflight->base.transaction, key, keylen, 0);

  // only call back on one of the futures; it'll chain to the other.
  // we'll set it on the dirent as anything returned there allows us to
  // abort and cancel the other future sooner.
  fdb_future_set_callback(inflight->dirent_check, fdbfs_error_checker, p);
}

void fdbfs_mknod(fuse_req_t req, fuse_ino_t ino,
		 const char *name, mode_t mode,
		 dev_t rdev)
{
  // get the file attributes of an inode
  struct fdbfs_inflight_mknod *inflight;
  int namelen = strlen(name);
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_mknod) +
				   sizeof(struct stat) +
				   namelen + 1,
				   req,
				   fdbfs_mknod_postverification,
				   fdbfs_mknod_issueverification,
				   T_READWRITE);
  inflight->parent = ino;
  inflight->name = ((char *)inflight) + sizeof(struct fdbfs_inflight_mknod);
  inflight->namelen = namelen;
  bcopy(name, inflight->name, namelen); // TODO smarter?
  inflight->mode = mode;
  inflight->rdev = rdev;

  debug_print("fdbfs_mknod taking off for req %p\n", inflight->base.req);
  
  fdbfs_mknod_issueverification(inflight);
}

void fdbfs_mkdir(fuse_req_t req, fuse_ino_t ino,
		 const char *name, mode_t mode)
{
  // get the file attributes of an inode
  struct fdbfs_inflight_mknod *inflight;
  int namelen = strlen(name);
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_mknod) +
				   sizeof(struct stat) +
				   namelen + 1,
				   req,
				   fdbfs_mknod_postverification,
				   fdbfs_mknod_issueverification,
				   T_READWRITE);
  inflight->parent = ino;
  inflight->name = ((char *)inflight) + sizeof(struct fdbfs_inflight_mknod);
  inflight->namelen = namelen;
  bcopy(name, inflight->name, namelen); // TODO smarter?
  inflight->mode = mode | S_IFDIR;
  inflight->rdev = 0;

  debug_print("fdbfs_mkdir taking off for req %p\n", inflight->base.req);

  fdbfs_mknod_issueverification(inflight);
}
