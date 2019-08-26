
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
 * setattr
 *************************************************************
 * INITIAL PLAN
 * get existing attributes.
 * apply to_set to inode_record
 * apply any side effects
 * repack inode_record, send to db
 *
 * REAL PLAN
 * ???
 */
struct fdbfs_inflight_setattr {
  struct fdbfs_inflight_base base;
  fuse_ino_t ino;
  struct stat attr;
  struct stat newattr;
  int to_set;
};

void fdbfs_setattr_commit_cb(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_setattr *inflight = p;
  fuse_reply_attr(inflight->base.req, &(inflight->newattr), 0.01);
  fdb_future_destroy(f);
  fdbfs_inflight_cleanup(p);
}

void fdbfs_setattr_callback(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_setattr *inflight = p;
  
  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  fdb_future_get_value(f, &present, (const uint8_t **)&val, &vallen);

  if(!present) {
    fdb_future_destroy(f);
    fuse_reply_err(inflight->base.req, EFAULT);
    fdbfs_inflight_cleanup(p);
    return;
  }

  INodeRecord *inode = inode_record__unpack(NULL, vallen, val);
  fdb_future_destroy(f);

  // update inode!

  Timespec atime = TIMESPEC__INIT,
    mtime = TIMESPEC__INIT;
  
  int mask_setuid_setgid = 0;
  if(inflight->to_set & FUSE_SET_ATTR_MODE) {
    inode->mode = inflight->attr.st_mode & 04777;
    inode->has_mode = 1;
  }
  if(inflight->to_set & FUSE_SET_ATTR_UID) {
    inode->uid = inflight->attr.st_uid;
    inode->has_uid = 1;
    mask_setuid_setgid = 1;
  }
  if(inflight->to_set & FUSE_SET_ATTR_GID) {
    inode->gid = inflight->attr.st_gid;
    inode->has_gid = 1;
    mask_setuid_setgid = 1;
  }
  if(inflight->to_set & FUSE_SET_ATTR_SIZE) {
    if(inflight->attr.st_size < inode->size) {
      // they want to truncate the file. compute what blocks
      // need to be cleared.
      uint8_t start_block_key[2048], stop_block_key[2048]; // TODO size
      int fileblock_key_len;
      pack_fileblock_key(inflight->ino,
			 (inflight->attr.st_size / BLOCKSIZE) + 1,
			 start_block_key, &fileblock_key_len);
      // we'll just clear to the last possible block
      pack_fileblock_key(inflight->ino, UINT64_MAX,
			 stop_block_key, &fileblock_key_len);
      fdb_transaction_clear_range(inflight->base.transaction,
				  start_block_key, fileblock_key_len,
				  stop_block_key, fileblock_key_len);
    }
    inode->size = inflight->attr.st_size;
    mask_setuid_setgid = 1;
  }
  if(inflight->to_set & FUSE_SET_ATTR_ATIME) {
    if(!(inode->atime))
      inode->atime = &atime;
    inode->atime->sec = inflight->attr.st_atim.tv_sec;
    inode->atime->nsec = inflight->attr.st_atim.tv_nsec;
    inode->atime->has_sec = inode->atime->has_nsec = 1;
  }
  if(inflight->to_set & FUSE_SET_ATTR_MTIME) {
    if(!(inode->mtime))
      inode->mtime = &mtime;
    inode->mtime->sec = inflight->attr.st_mtim.tv_sec;
    inode->mtime->nsec = inflight->attr.st_mtim.tv_nsec;
    inode->mtime->has_sec = inode->mtime->has_nsec = 1;
  }
#ifdef FUSE_SET_ATTR_CTIME
  if(inflight->to_set & FUSE_SET_ATTR_CTIME) {
    Timespec ctime = TIMESPEC__INIT;
    if(!(inode->ctime))
      inode->ctime = &ctime;
    inode->ctime->sec = inflight->attr.st_ctim.tv_sec;
    inode->ctime->nsec = inflight->attr.st_ctim.tv_nsec;
    inode->ctime->has_sec = inode->ctime->has_nsec = 1;
  }
#endif
  if((inflight->to_set & FUSE_SET_ATTR_ATIME_NOW) ||
     (inflight->to_set & FUSE_SET_ATTR_MTIME_NOW)) {
    struct timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    if(inflight->to_set & FUSE_SET_ATTR_ATIME_NOW) {
      if(!(inode->atime))
	inode->atime = &atime;
      inode->atime->sec = tp.tv_sec;
      inode->atime->nsec = tp.tv_nsec;
      inode->atime->has_sec = inode->atime->has_nsec = 1;
    }
    if(inflight->to_set & FUSE_SET_ATTR_MTIME_NOW) {
      if(!(inode->mtime))
	inode->mtime = &mtime;
      inode->mtime->sec = tp.tv_sec;
      inode->mtime->nsec = tp.tv_nsec;
      inode->mtime->has_sec = inode->mtime->has_nsec = 1;
    }
  }

  if(mask_setuid_setgid) {
    inode->mode = inode->mode & 01777;
    inode->has_mode = 1;
  }
  // done updating inode!


  // repack for fuse
  pack_inode_record_into_stat(inode, &(inflight->newattr));
  
  int inode_size = inode_record__get_packed_size(inode);
  uint8_t inode_buffer[inode_size];
  inode_record__pack(inode, inode_buffer);
  inode_record__free_unpacked(inode, NULL);

  uint8_t key[512];
  int keylen;
  pack_inode_key(inflight->ino, key, &keylen);

  fdb_transaction_set(inflight->base.transaction,
		      key, keylen,
		      inode_buffer, inode_size);

  inflight->base.cb = fdbfs_setattr_commit_cb;
  fdb_future_set_callback(fdb_transaction_commit(inflight->base.transaction),
			  fdbfs_error_checker, p);
}

void fdbfs_setattr_issuer(void *p)
{
  struct fdbfs_inflight_setattr *inflight = p;

  // pack the inode key
  uint8_t key[512];
  int keylen;

  pack_inode_key(inflight->ino, key, &keylen);

  // and request just that inode
  FDBFuture *f = fdb_transaction_get(inflight->base.transaction, key, keylen, 0);

  fdb_future_set_callback(f, fdbfs_error_checker, p);
}

void fdbfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
		   int to_set, struct fuse_file_info *fi)
{
  // set the file attributes of an inode
  struct fdbfs_inflight_setattr *inflight;
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_setattr),
				   req,
				   fdbfs_setattr_callback,
				   fdbfs_setattr_issuer,
				   T_READWRITE);
  inflight->ino = ino;
  inflight->attr = *attr;
  inflight->to_set = to_set;

  fdbfs_setattr_issuer(inflight);
}
