
#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 610
#include <foundationdb/fdb_c.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
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
class Inflight_setattr : public Inflight {
public:
  Inflight_setattr(fuse_req_t, fuse_ino_t, struct stat, int,
		   unique_transaction);
  Inflight_setattr *reincarnate();
  InflightCallback issue();
private:
  fuse_ino_t ino;
  struct stat attr;
  std::unique_ptr<struct stat> newattr;
  int to_set;

  unique_future inode_fetch;
  InflightAction callback();
  unique_future commit;
  InflightAction commit_cb();
};

Inflight_setattr::Inflight_setattr(fuse_req_t req, fuse_ino_t ino,
				   struct stat attr, int to_set,
				   unique_transaction transaction)
  : Inflight(req, true, std::move(transaction)),
    ino(ino), attr(attr), to_set(to_set)
{
}

Inflight_setattr *Inflight_setattr::reincarnate()
{
  Inflight_setattr *x = new Inflight_setattr(req, ino, attr, to_set,
					     std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_setattr::commit_cb()
{
  return InflightAction::Attr(std::move(newattr));
}

InflightAction Inflight_setattr::callback()
{
  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(inode_fetch.get(), &present, (const uint8_t **)&val, &vallen);
  if(err) return InflightAction::FDBError(err);

  if(!present) {
    return InflightAction::Abort(EFAULT);
  }

  INodeRecord inode;
  inode.ParseFromArray(val, vallen);
  if(!inode.IsInitialized()) {
    // bad inode
    return InflightAction::Abort(EIO);
  }

  // update inode!

  int mask_setuid_setgid = 0;
  if(to_set & FUSE_SET_ATTR_MODE) {
    inode.set_mode(attr.st_mode & 04777);
  }
  if(to_set & FUSE_SET_ATTR_UID) {
    inode.set_uid(attr.st_uid);
    mask_setuid_setgid = 1;
  }
  if(to_set & FUSE_SET_ATTR_GID) {
    inode.set_gid(attr.st_gid);
    mask_setuid_setgid = 1;
  }
  if(to_set & FUSE_SET_ATTR_SIZE) {
    if(attr.st_size < inode.size()) {
      // they want to truncate the file. compute what blocks
      // need to be cleared.
      auto start_block_key = pack_fileblock_key(ino, (attr.st_size / BLOCKSIZE) + 1);
      // we'll just clear to the last possible block
      auto stop_block_key = pack_fileblock_key(ino, UINT64_MAX);
      fdb_transaction_clear_range(transaction.get(),
				  start_block_key.data(), start_block_key.size(),
				  stop_block_key.data(), stop_block_key.size());
    }
    inode.set_size(attr.st_size);
    mask_setuid_setgid = 1;
  }
  if(to_set & FUSE_SET_ATTR_ATIME) {
    inode.mutable_atime()->set_sec(attr.st_atim.tv_sec);
    inode.mutable_atime()->set_nsec(attr.st_atim.tv_nsec);
  }
  if(to_set & FUSE_SET_ATTR_MTIME) {
    inode.mutable_mtime()->set_sec(attr.st_mtim.tv_sec);
    inode.mutable_mtime()->set_nsec(attr.st_mtim.tv_nsec);
  }
#ifdef FUSE_SET_ATTR_CTIME
  if(to_set & FUSE_SET_ATTR_CTIME) {
    inode.mutable_ctime()->set_sec(attr.st_ctim.tv_sec);
    inode.mutable_ctime()->set_nsec(attr.st_ctim.tv_nsec);
  }
#endif
  if((to_set & FUSE_SET_ATTR_ATIME_NOW) ||
     (to_set & FUSE_SET_ATTR_MTIME_NOW)) {
    struct timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    if(to_set & FUSE_SET_ATTR_ATIME_NOW) {
      inode.mutable_atime()->set_sec(tp.tv_sec);
      inode.mutable_atime()->set_nsec(tp.tv_nsec);
    }
    if(to_set & FUSE_SET_ATTR_MTIME_NOW) {
      inode.mutable_mtime()->set_sec(tp.tv_sec);
      inode.mutable_mtime()->set_nsec(tp.tv_nsec);
    }
  }

  if(mask_setuid_setgid) {
    inode.set_mode(inode.mode() & 01777);
  }
  // done updating inode!


  // repack for fuse
  newattr = std::make_unique<struct stat>();
  pack_inode_record_into_stat(&inode, newattr.get());
  
  int inode_size = inode.ByteSize();
  uint8_t inode_buffer[inode_size];
  inode.SerializeToArray(inode_buffer, inode_size);

  auto key = pack_inode_key(ino);

  fdb_transaction_set(transaction.get(),
		      key.data(), key.size(),
		      inode_buffer, inode_size);

  wait_on_future(fdb_transaction_commit(transaction.get()),
		 &commit);

  return InflightAction::BeginWait(std::bind(&Inflight_setattr::commit_cb, this));
}

InflightCallback Inflight_setattr::issue()
{
  auto key = pack_inode_key(ino);

  // and request just that inode
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &inode_fetch);
  return std::bind(&Inflight_setattr::callback, this);
}

extern "C" void fdbfs_setattr(fuse_req_t req, fuse_ino_t ino,
			      struct stat *attr,
			      int to_set, struct fuse_file_info *fi)
{
  Inflight_setattr *inflight =
    new Inflight_setattr(req, ino, *attr, to_set, make_transaction());
  inflight->start();
}
