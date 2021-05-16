
#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>

#include "util.h"
#include "inflight.h"
#include "fdbfs_ops.h"

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
  unique_future partial_block_fetch;
  uint64_t partial_block_idx;
  InflightAction partial_block_fixup();
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

InflightAction Inflight_setattr::partial_block_fixup()
{
  FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_error_t err;
  err = fdb_future_get_keyvalue_array(partial_block_fetch.get(), (const FDBKeyValue **)&kvs, &kvcount, &more);
  if(err) return InflightAction::FDBError(err);

  if(kvcount>0) {
    // there's a block there, decode it, and rewrite a truncated version
    uint8_t output_buffer[BLOCKSIZE];
    bzero(output_buffer, BLOCKSIZE);
    int ret = decode_block(&kvs[0], 0, output_buffer, BLOCKSIZE, BLOCKSIZE);
    auto key = pack_fileblock_key(ino, partial_block_idx);
    set_block(transaction.get(), key, output_buffer, attr.st_size % BLOCKSIZE);
  }

  wait_on_future(fdb_transaction_commit(transaction.get()),
		 &commit);
  return InflightAction::BeginWait(std::bind(&Inflight_setattr::commit_cb, this));
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

  bool do_commit = true;
  auto next_action = InflightAction::BeginWait(std::bind(&Inflight_setattr::commit_cb, this));

  INodeRecord inode;
  inode.ParseFromArray(val, vallen);
  if(!inode.IsInitialized()) {
    // bad inode
    return InflightAction::Abort(EIO);
  }

  // update inode!

  bool substantial_update = false;
  if(to_set & FUSE_SET_ATTR_MODE) {
    inode.set_mode(attr.st_mode & 07777);
    substantial_update = true;
  }
  if(to_set & FUSE_SET_ATTR_UID) {
    inode.set_uid(attr.st_uid);
    substantial_update = true;
  }
  if(to_set & FUSE_SET_ATTR_GID) {
    inode.set_gid(attr.st_gid);
    substantial_update = true;
  }
  if(to_set & FUSE_SET_ATTR_SIZE) {
    if(attr.st_size < inode.size()) {
      // they want to truncate the file. compute what blocks
      // need to be cleared.
      auto start_block_key = pack_fileblock_key(ino, (attr.st_size / BLOCKSIZE));
      // we'll just clear to the last possible block
      auto stop_block_key = pack_fileblock_key(ino, UINT64_MAX);
      fdb_transaction_clear_range(transaction.get(),
				  start_block_key.data(), start_block_key.size(),
				  stop_block_key.data(), stop_block_key.size());

      if((attr.st_size % BLOCKSIZE) != 0) {
	// we should have a partially truncated block
	// we're responsible for reading it and writing a corrected version
	partial_block_idx = attr.st_size / BLOCKSIZE;
	auto start_key = pack_fileblock_key(ino, partial_block_idx);
	auto stop_key = start_key;
	stop_key.push_back(0xff);
	wait_on_future(fdb_transaction_get_range(transaction.get(),
						 FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start_key.data(), start_key.size()),
						 FDB_KEYSEL_FIRST_GREATER_THAN(stop_key.data(), stop_key.size()),
						 0, 0,
						 FDB_STREAMING_MODE_WANT_ALL,
						 0, 0, 0),
		       &partial_block_fetch);
	do_commit = false;
	next_action = InflightAction::BeginWait(std::bind(&Inflight_setattr::partial_block_fixup, this));
      }
    }
    if(attr.st_size != inode.size()) {
      inode.set_size(attr.st_size);
      substantial_update = true;
    }
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
     (to_set & FUSE_SET_ATTR_MTIME_NOW) ||
     substantial_update) {
    struct timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    if(to_set & FUSE_SET_ATTR_ATIME_NOW) {
      inode.mutable_atime()->set_sec(tp.tv_sec);
      inode.mutable_atime()->set_nsec(tp.tv_nsec);
    }
    if((to_set & FUSE_SET_ATTR_MTIME_NOW) || substantial_update) {
      inode.mutable_mtime()->set_sec(tp.tv_sec);
      inode.mutable_mtime()->set_nsec(tp.tv_nsec);
    }
    if(substantial_update) {
      inode.mutable_ctime()->set_sec(tp.tv_sec);
      inode.mutable_ctime()->set_nsec(tp.tv_nsec);
    }
  }

  if(substantial_update &&
     !(to_set & FUSE_SET_ATTR_MODE) &&
     !(inode.has_type() && inode.type() == directory)) {
    // strip setuid and setgid unless we just updated the mode,
    // or we're operating on a directory.
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

  if(do_commit) {
    wait_on_future(fdb_transaction_commit(transaction.get()),
		   &commit);
  }

  return next_action;
}

InflightCallback Inflight_setattr::issue()
{
  // turn off RYW, so there's no uncertainty about what we'll get when
  // we interleave our reads and writes.
  if(fdb_transaction_set_option(transaction.get(),
				FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE,
				NULL, 0)) {
    // hmm.
    // TODO how do we generate an error here?
    return []() {
      // i don't think this will be run, since we've registered no futures?
      return InflightAction::Abort(EIO);
    };
  }

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
