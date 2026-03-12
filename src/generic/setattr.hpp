
#define FUSE_USE_VERSION 35
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <variant>

#include "filehandle.h"
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
template <typename ActionT>
struct AttemptState_setattr : public AttemptStateT<ActionT> {
  INodeRecord inode;
  unique_future inode_fetch;
  unique_future partial_block_fetch;
  uint64_t partial_block_idx = 0;
};

template <typename ActionT>
class Inflight_setattr
    : public InflightWithAttemptT<AttemptState_setattr<ActionT>,
                                  InflightPolicyWrite, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_setattr<ActionT>,
                                    InflightPolicyWrite, ActionT>;
  using Base::a;
  using Base::commit;
  using Base::track_inode_for_fsync;
  using Base::transaction;
  using Base::wait_on_future;
  using Base::write_oplog_result;

  struct SuccessReplyAttr {};
  struct SuccessReplyOpen {
    struct fuse_file_info fi;
  };
  using SuccessReply = std::variant<SuccessReplyAttr, SuccessReplyOpen>;

  Inflight_setattr(fuse_req_t, fuse_ino_t, struct stat, int, unique_transaction,
                   SuccessReply = SuccessReplyAttr{});
  InflightCallbackT<ActionT> issue();

private:
  const fuse_ino_t ino;
  const struct stat attr{};
  const int to_set;
  const SuccessReply success_reply;

  fdb_error_t configure_transaction() override;
  ActionT callback();
  ActionT partial_block_fixup();
  ActionT commit_cb();
  ActionT oplog_recovery(const OpLogRecord &) override;
  bool write_success_oplog_result();
};

template <typename ActionT>
Inflight_setattr<ActionT>::Inflight_setattr(fuse_req_t req, fuse_ino_t ino,
                                            struct stat attr, int to_set,
                                            unique_transaction transaction,
                                            SuccessReply success_reply)
    : Base(req, std::move(transaction)), ino(ino), attr(attr),
      to_set(to_set), success_reply(std::move(success_reply)) {
  track_inode_for_fsync(ino);
}

template <typename ActionT>
fdb_error_t Inflight_setattr<ActionT>::configure_transaction() {
  return fdb_transaction_set_option(
      transaction.get(), FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, nullptr, 0);
}

template <typename ActionT>
ActionT Inflight_setattr<ActionT>::commit_cb() {
  auto open_reply = std::get_if<SuccessReplyOpen>(&success_reply);
  if (open_reply != nullptr) {
    // we copy fi because success_reply is const
    struct fuse_file_info fi = open_reply->fi;
    return ActionT::Open(ino, fi);
  }
  struct stat newattr{};
  pack_inode_record_into_stat(a().inode, newattr);
  return ActionT::Attr(newattr);
}

template <typename ActionT>
bool Inflight_setattr<ActionT>::write_success_oplog_result() {
  auto open_reply = std::get_if<SuccessReplyOpen>(&success_reply);
  if (open_reply != nullptr) {
    OpLogResultOpen result;
    result.set_ino(ino);
    result.set_flags(open_reply->fi.flags);
    result.set_direct_io(open_reply->fi.direct_io);
    result.set_keep_cache(open_reply->fi.keep_cache);
    result.set_nonseekable(open_reply->fi.nonseekable);
    return write_oplog_result(result);
  }

  struct stat newattr{};
  pack_inode_record_into_stat(a().inode, newattr);
  OpLogResultAttr result;
  *result.mutable_attr() = pack_stat_into_stat_record(newattr);
  return write_oplog_result(result);
}

template <typename ActionT>
ActionT Inflight_setattr<ActionT>::oplog_recovery(const OpLogRecord &record) {
  switch (record.result_case()) {
  case OpLogRecord::kAttr: {
    if (!record.attr().has_attr()) {
      return ActionT::Abort(EIO);
    }
    struct stat attr{};
    unpack_stat_record_into_stat(record.attr().attr(), attr);
    return ActionT::Attr(attr);
  }
  case OpLogRecord::kOpen: {
    struct fuse_file_info fi{};
    fi.flags = record.open().flags();
    fi.direct_io = record.open().direct_io();
    fi.keep_cache = record.open().keep_cache();
    fi.nonseekable = record.open().nonseekable();
    return ActionT::Open(record.open().ino(), fi);
  }
  default:
    return ActionT::Abort(EIO);
  }
}

template <typename ActionT>
ActionT Inflight_setattr<ActionT>::partial_block_fixup() {
  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_error_t err;
  err = fdb_future_get_keyvalue_array(a().partial_block_fetch.get(), &kvs,
                                      &kvcount, &more);
  if (err)
    return ActionT::FDBError(err);

  if (kvcount > 0) {
    // there's a block there, decode it, and rewrite a truncated version
    std::vector<uint8_t> output_buffer(BLOCKSIZE, 0);
    const auto dret =
        decode_block(&kvs[0], 0, std::span<uint8_t>(output_buffer), BLOCKSIZE);
    if (!dret) {
      // block can't be decoded. big problem.
      return ActionT::Abort(EIO);
    }
    auto key = pack_fileblock_key(ino, a().partial_block_idx);
    // if (ret <= attr.st_size % BLOCKSIZE) then there's nothing to do.
    const auto write_size = static_cast<size_t>(attr.st_size % BLOCKSIZE);
    const auto sret = set_fileblock(
        transaction.get(), key,
        std::span<const uint8_t>(output_buffer).first(write_size));
    if (!sret)
      return ActionT::Abort(EIO);
  }

  if (!write_success_oplog_result()) {
    return ActionT::Abort(EIO);
  }

  return commit(std::bind(&Inflight_setattr<ActionT>::commit_cb, this));
}

template <typename ActionT>
ActionT Inflight_setattr<ActionT>::callback() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(a().inode_fetch.get(), &present, &val, &vallen);
  if (err)
    return ActionT::FDBError(err);

  if (!present) {
    return ActionT::Abort(ENOENT);
  }

  bool do_commit = true;
  auto next_action =
      ActionT::BeginWait(std::bind(&Inflight_setattr<ActionT>::commit_cb, this));

  a().inode.ParseFromArray(val, vallen);
  if (!a().inode.IsInitialized()) {
    // bad inode
    return ActionT::Abort(EIO);
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
      const auto range = pack_fileblock_span_range(
          ino, (attr.st_size / BLOCKSIZE), UINT64_MAX);
      fdbfs_transaction_clear_range(transaction.get(), range);

      if ((attr.st_size % BLOCKSIZE) != 0) {
        // we should have a partially truncated block
        // we're responsible for reading it and writing a corrected version
        a().partial_block_idx = attr.st_size / BLOCKSIZE;
        const auto [start_key, stop_key] =
            pack_fileblock_single_range(ino, a().partial_block_idx);
        wait_on_future(
            fdb_transaction_get_range(
                transaction.get(),
                FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start_key.data(),
                                                  start_key.size()),
                FDB_KEYSEL_FIRST_GREATER_THAN(stop_key.data(), stop_key.size()),
                0, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
            a().partial_block_fetch);
        do_commit = false;
        next_action = ActionT::BeginWait(
            std::bind(&Inflight_setattr<ActionT>::partial_block_fixup, this));
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
    return ActionT::Abort(EIO);

  if (do_commit) {
    if (!write_success_oplog_result()) {
      return ActionT::Abort(EIO);
    }
    wait_on_future(fdb_transaction_commit(transaction.get()), a().commit);
  }

  return next_action;
}

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_setattr<ActionT>::issue() {
  const auto key = pack_inode_key(ino);

  // and request just that inode
  wait_on_future(
      fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
      a().inode_fetch);
  return std::bind(&Inflight_setattr<ActionT>::callback, this);
}
