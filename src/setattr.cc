
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <variant>

#include "fdbfs_ops.h"
#include "inflight.h"
#include "util.h"

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
struct AttemptState_setattr : public AttemptState {
  INodeRecord inode;
  unique_future inode_fetch;
  unique_future partial_block_fetch;
  uint64_t partial_block_idx = 0;
};

class Inflight_setattr : public InflightWithAttempt<AttemptState_setattr> {
public:
  struct SuccessReplyAttr {};
  struct SuccessReplyOpen {
    struct fuse_file_info fi;
  };
  using SuccessReply = std::variant<SuccessReplyAttr, SuccessReplyOpen>;

  Inflight_setattr(fuse_req_t, fuse_ino_t, struct stat, int, unique_transaction,
                   SuccessReply = SuccessReplyAttr{});
  InflightCallback issue();

private:
  const fuse_ino_t ino;
  const struct stat attr{};
  const int to_set;
  const SuccessReply success_reply;

  InflightAction callback();
  InflightAction partial_block_fixup();
  InflightAction commit_cb();
};

Inflight_setattr::Inflight_setattr(fuse_req_t req, fuse_ino_t ino,
                                   struct stat attr, int to_set,
                                   unique_transaction transaction,
                                   SuccessReply success_reply)
    : InflightWithAttempt(req, ReadWrite::Yes, std::move(transaction)),
      ino(ino), attr(attr), to_set(to_set),
      success_reply(std::move(success_reply)) {}

InflightAction Inflight_setattr::commit_cb() {
  auto open_reply = std::get_if<SuccessReplyOpen>(&success_reply);
  if (open_reply != nullptr) {
    // we copy fi because success_reply is const
    struct fuse_file_info fi = open_reply->fi;
    return InflightAction::Open(ino, fi);
  }
  struct stat newattr{};
  pack_inode_record_into_stat(a().inode, newattr);
  return InflightAction::Attr(newattr);
}

InflightAction Inflight_setattr::partial_block_fixup() {
  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_error_t err;
  err = fdb_future_get_keyvalue_array(a().partial_block_fetch.get(), &kvs,
                                      &kvcount, &more);
  if (err)
    return InflightAction::FDBError(err);

  if (kvcount > 0) {
    // there's a block there, decode it, and rewrite a truncated version
    std::vector<uint8_t> output_buffer(BLOCKSIZE, 0);
    const auto dret =
        decode_block(&kvs[0], 0, std::span<uint8_t>(output_buffer), BLOCKSIZE);
    if (!dret) {
      // block can't be decoded. big problem.
      return InflightAction::Abort(EIO);
    }
    auto key = pack_fileblock_key(ino, a().partial_block_idx);
    // if (ret <= attr.st_size % BLOCKSIZE) then there's nothing to do.
    const auto write_size = static_cast<size_t>(attr.st_size % BLOCKSIZE);
    const auto sret =
        set_block(transaction.get(), key,
                  std::span<const uint8_t>(output_buffer).first(write_size));
    if (!sret)
      return InflightAction::Abort(EIO);
  }

  return commit(std::bind(&Inflight_setattr::commit_cb, this));
}

InflightAction Inflight_setattr::callback() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(a().inode_fetch.get(), &present, &val, &vallen);
  if (err)
    return InflightAction::FDBError(err);

  if (!present) {
    return InflightAction::Abort(ENOENT);
  }

  bool do_commit = true;
  auto next_action =
      InflightAction::BeginWait(std::bind(&Inflight_setattr::commit_cb, this));

  a().inode.ParseFromArray(val, vallen);
  if (!a().inode.IsInitialized()) {
    // bad inode
    return InflightAction::Abort(EIO);
  }

  // update inode!

  bool substantial_update = false;
  if (to_set & FUSE_SET_ATTR_MODE) {
    a().inode.set_mode(attr.st_mode & 07777);
    substantial_update = true;
  }
  if (to_set & FUSE_SET_ATTR_UID) {
    a().inode.set_uid(attr.st_uid);
    substantial_update = true;
  }
  if (to_set & FUSE_SET_ATTR_GID) {
    a().inode.set_gid(attr.st_gid);
    substantial_update = true;
  }
  if (to_set & FUSE_SET_ATTR_SIZE) {
    if (static_cast<uint64_t>(attr.st_size) < a().inode.size()) {
      // they want to truncate the file. compute what blocks
      // need to be cleared.
      auto start_block_key =
          pack_fileblock_key(ino, (attr.st_size / BLOCKSIZE));
      // we'll just clear to the last possible block
      auto stop_block_key = pack_fileblock_key(ino, UINT64_MAX);
      fdb_transaction_clear_range(transaction.get(), start_block_key.data(),
                                  start_block_key.size(), stop_block_key.data(),
                                  stop_block_key.size());

      if ((attr.st_size % BLOCKSIZE) != 0) {
        // we should have a partially truncated block
        // we're responsible for reading it and writing a corrected version
        a().partial_block_idx = attr.st_size / BLOCKSIZE;
        auto start_key = pack_fileblock_key(ino, a().partial_block_idx);
        auto stop_key = start_key;
        stop_key.push_back(0xff);
        wait_on_future(
            fdb_transaction_get_range(
                transaction.get(),
                FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start_key.data(),
                                                  start_key.size()),
                FDB_KEYSEL_FIRST_GREATER_THAN(stop_key.data(), stop_key.size()),
                0, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
            a().partial_block_fetch);
        do_commit = false;
        next_action = InflightAction::BeginWait(
            std::bind(&Inflight_setattr::partial_block_fixup, this));
      }
    }
    if (static_cast<uint64_t>(attr.st_size) != a().inode.size()) {
      a().inode.set_size(attr.st_size);
      substantial_update = true;
    }
  }
  if (to_set & FUSE_SET_ATTR_ATIME) {
    a().inode.mutable_atime()->set_sec(attr.st_atim.tv_sec);
    a().inode.mutable_atime()->set_nsec(attr.st_atim.tv_nsec);
  }
  if (to_set & FUSE_SET_ATTR_MTIME) {
    a().inode.mutable_mtime()->set_sec(attr.st_mtim.tv_sec);
    a().inode.mutable_mtime()->set_nsec(attr.st_mtim.tv_nsec);
  }
#ifdef FUSE_SET_ATTR_CTIME
  if (to_set & FUSE_SET_ATTR_CTIME) {
    a().inode.mutable_ctime()->set_sec(attr.st_ctim.tv_sec);
    a().inode.mutable_ctime()->set_nsec(attr.st_ctim.tv_nsec);
  }
#endif

  if ((to_set & FUSE_SET_ATTR_ATIME_NOW) ||
      (to_set & FUSE_SET_ATTR_MTIME_NOW) || substantial_update) {
    struct timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    if (to_set & FUSE_SET_ATTR_ATIME_NOW) {
      a().inode.mutable_atime()->set_sec(tp.tv_sec);
      a().inode.mutable_atime()->set_nsec(tp.tv_nsec);
    }
    if ((to_set & FUSE_SET_ATTR_MTIME_NOW) || substantial_update) {
      a().inode.mutable_mtime()->set_sec(tp.tv_sec);
      a().inode.mutable_mtime()->set_nsec(tp.tv_nsec);
    }
    if (substantial_update) {
      a().inode.mutable_ctime()->set_sec(tp.tv_sec);
      a().inode.mutable_ctime()->set_nsec(tp.tv_nsec);
    }
  }

  if (substantial_update && !(to_set & FUSE_SET_ATTR_MODE) &&
      !(a().inode.has_type() && a().inode.type() == ft_directory)) {
    // strip setuid and setgid unless we just updated the mode,
    // or we're operating on a directory.
    a().inode.set_mode(a().inode.mode() & 01777);
  }
  // done updating inode!

  if (!fdb_set_protobuf(transaction.get(), pack_inode_key(ino), a().inode))
    return InflightAction::Abort(EIO);

  if (do_commit) {
    wait_on_future(fdb_transaction_commit(transaction.get()), a().commit);
  }

  return next_action;
}

InflightCallback Inflight_setattr::issue() {
  // turn off RYW, so there's no uncertainty about what we'll get when
  // we interleave our reads and writes.
  if (fdb_transaction_set_option(
          transaction.get(), FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, NULL, 0)) {
    return []() { return InflightAction::Abort(EIO); };
  }

  const auto key = pack_inode_key(ino);

  // and request just that inode
  wait_on_future(
      fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
      a().inode_fetch);
  return std::bind(&Inflight_setattr::callback, this);
}

extern "C" void fdbfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                              int to_set, struct fuse_file_info *fi) {
  Inflight_setattr *inflight =
      new Inflight_setattr(req, ino, *attr, to_set, make_transaction());
  inflight->start();
}

extern "C" void fdbfs_setattr_open_trunc(fuse_req_t req, fuse_ino_t ino,
                                         struct fuse_file_info *fi) {
  struct stat attr{};
  attr.st_size = 0;
  Inflight_setattr *inflight = new Inflight_setattr(
      req, ino, attr, FUSE_SET_ATTR_SIZE, make_transaction(),
      Inflight_setattr::SuccessReplyOpen{*fi});
  inflight->start();
}
