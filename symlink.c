
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
#include "values.pb-c.h"

/*************************************************************
 * symlink
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
struct fdbfs_inflight_symlink {
  struct fdbfs_inflight_base base;
  FDBFuture *inode_check;
  FDBFuture *dirent_check;
  fuse_ino_t parent;
  fuse_ino_t ino;
  struct stat attr;
  char *name;
  int namelen;
  char *link;
  int linklen;
};

void fdbfs_symlink_commit_cb(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_symlink *inflight = p;
  
  struct fuse_entry_param e;
  bzero(&e, sizeof(struct fuse_entry_param));
  e.ino = inflight->ino;
  e.generation = 1;
  bcopy(&(inflight->attr), &(e.attr), sizeof(struct stat));
  e.attr_timeout = 0.01;
  e.entry_timeout = 0.01;

  debug_print("fdbfs_symlink_commit_cb returning ino %lx ino %lx\n", e.ino, e.attr.st_ino);
  
  int ret = fuse_reply_entry(inflight->base.req, &e);
  debug_print("fdbfs_symlink_commit_cb fuse_reply_entry returned %i\n", ret);
  fdb_future_destroy(f);
  fdbfs_inflight_cleanup(p);
}

void fdbfs_symlink_issueverification(void *p);

void fdbfs_symlink_postverification(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_symlink *inflight = p;

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
    fdbfs_symlink_issueverification(p);
    return;
  }

  INodeRecord inode = INODE_RECORD__INIT;
  inode.inode = inflight->ino;
  inode.type = FILETYPE__symlink;
  inode.nlinks = 1;
  inode.symlink = inflight->link;
  inode.has_nlinks = inode.has_inode = inode.has_type = 1;

  Timespec atime = TIMESPEC__INIT,
    mtime = TIMESPEC__INIT,
    ctime = TIMESPEC__INIT;
  inode.atime = &atime;
  inode.atime->sec = 1565989127;
  inode.atime->nsec = 0;
  inode.atime->has_sec = inode.atime->has_nsec = 1;

  inode.mtime = &mtime;
  inode.mtime->sec = 1565989127;
  inode.mtime->nsec = 0;
  inode.mtime->has_sec = inode.mtime->has_nsec = 1;

  inode.ctime = &ctime;
  inode.ctime->sec = 1565989127;
  inode.ctime->nsec = 0;
  inode.ctime->has_sec = inode.ctime->has_nsec = 1;

  // wrap it up to be returned to fuse later
  pack_inode_record_into_stat(&inode, &(inflight->attr));
  
  // set the inode KV pair
  uint8_t key[2048]; int keylen;
  pack_inode_key(inflight->ino, key, &keylen);
  int inode_size = inode_record__get_packed_size(&inode);
  uint8_t inode_buffer[inode_size];
  inode_record__pack(&inode, inode_buffer);
  
  fdb_transaction_set(inflight->base.transaction,
		      key, keylen,
		      inode_buffer, inode_size);
  
  DirectoryEntry dirent = DIRECTORY_ENTRY__INIT;
  dirent.inode = inflight->ino;
  dirent.type = FILETYPE__symlink;
  dirent.has_inode = dirent.has_type = 1;

  int dirent_size = directory_entry__get_packed_size(&dirent);
  uint8_t dirent_buffer[dirent_size];
  directory_entry__pack(&dirent, dirent_buffer);

  printf("SYMLINK.C dirent size %i\n", dirent_size);
  pack_dentry_key(inflight->parent, inflight->name, inflight->namelen, key, &keylen);
  fdb_transaction_set(inflight->base.transaction,
		      key, keylen,
		      dirent_buffer, dirent_size);

  // if the commit works, we can reply to fuse and clean up
  // if it doesn't, the issuer will try again.
  inflight->base.cb = fdbfs_symlink_commit_cb;
  fdb_future_set_callback(fdb_transaction_commit(inflight->base.transaction),
			  fdbfs_error_checker, p);
}

void fdbfs_symlink_issueverification(void *p)
{
  struct fdbfs_inflight_symlink *inflight = p;

  inflight->ino = generate_inode();
  // reset this in case commit failed
  inflight->base.cb = fdbfs_symlink_postverification;

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

void fdbfs_symlink(fuse_req_t req, const char *link,
		   fuse_ino_t parent, const char *name)
{
  // get the file attributes of an inode
  struct fdbfs_inflight_symlink *inflight;
  int namelen = strlen(name);
  int linklen = strlen(link);
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_symlink) +
				   linklen + 1 +
				   namelen + 1,
				   req,
				   fdbfs_symlink_postverification,
				   fdbfs_symlink_issueverification,
				   T_READWRITE);
  inflight->parent = parent;

  inflight->name = ((char *)inflight) +
    sizeof(struct fdbfs_inflight_symlink);
  inflight->namelen = namelen;

  inflight->link = inflight->name + namelen + 1;
  inflight->linklen = linklen;

  bcopy(name, inflight->name, namelen);
  *(inflight->name+namelen) = '\0';
  bcopy(link, inflight->link, linklen);
  *(inflight->link+linklen) = '\0';

  debug_print("fdbfs_symlink taking off for req %p\n", inflight->base.req);

  fdbfs_symlink_issueverification(inflight);
}
