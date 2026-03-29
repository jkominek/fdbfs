
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <variant>

#include "inflight.h"
#include "util.h"

enum class SetAttrBit : uint32_t {
  Mode = (1u << 0),
  Uid = (1u << 1),
  Gid = (1u << 2),
  Size = (1u << 3),
  Atime = (1u << 4),
  Mtime = (1u << 5),
  AtimeNow = (1u << 7),
  MtimeNow = (1u << 8),
  Force = (1u << 9),
  Ctime = (1u << 10),
  KillSuid = (1u << 11),
  KillSgid = (1u << 12),
  File = (1u << 13),
  KillPriv = (1u << 14),
  Open = (1u << 15),
  TimesSet = (1u << 16),
  Touch = (1u << 17),
};

class SetAttrMask {
public:
  constexpr SetAttrMask() = default;
  constexpr explicit SetAttrMask(SetAttrBit bit)
      : bits_(static_cast<uint32_t>(bit)) {}
  constexpr explicit SetAttrMask(uint32_t bits) : bits_(bits) {}

  [[nodiscard]] static constexpr SetAttrMask from_raw(uint32_t bits) {
    return SetAttrMask(bits);
  }

  [[nodiscard]] constexpr uint32_t raw() const { return bits_; }
  [[nodiscard]] constexpr bool has(SetAttrBit bit) const {
    return (bits_ & static_cast<uint32_t>(bit)) != 0;
  }

private:
  uint32_t bits_ = 0;
};

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

template <typename ActionT, typename INodeHandlerT>
class Inflight_setattr
    : public InflightWithAttemptT<AttemptState_setattr<ActionT>,
                                  InflightPolicyWrite, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_setattr<ActionT>,
                                    InflightPolicyWrite, ActionT>;
  using req_t = typename ActionT::req_t;
  using Base::a;
  using Base::commit;
  using Base::track_inode_for_fsync;
  using Base::transaction;
  using Base::wait_on_future;
  using Base::write_oplog_result;

  Inflight_setattr(req_t, fdbfs_ino_t, struct stat, SetAttrMask,
                   unique_transaction, INodeHandlerT);
  InflightCallbackT<ActionT> issue();

private:
  const fdbfs_ino_t ino;
  const struct stat attr{};
  const SetAttrMask to_set;
  const INodeHandlerT inode_handler;

  fdb_error_t configure_transaction() override;
  ActionT callback();
  ActionT partial_block_fixup();
  ActionT commit_cb();
  ActionT oplog_recovery(const OpLogRecord &) override;
  bool write_success_oplog_result();
};

template <typename ActionT, typename INodeHandlerT>
Inflight_setattr<ActionT, INodeHandlerT>::Inflight_setattr(
    req_t req, fdbfs_ino_t ino, struct stat attr, SetAttrMask to_set,
    unique_transaction transaction, INodeHandlerT inode_handler)
    : Base(req, std::move(transaction)), ino(ino), attr(attr), to_set(to_set),
      inode_handler(std::move(inode_handler)) {
  track_inode_for_fsync(ino);
}

template <typename ActionT, typename INodeHandlerT>
fdb_error_t Inflight_setattr<ActionT, INodeHandlerT>::configure_transaction() {
  return fdb_transaction_set_option(
      transaction.get(), FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, nullptr, 0);
}

template <typename ActionT, typename INodeHandlerT>
ActionT Inflight_setattr<ActionT, INodeHandlerT>::commit_cb() {
  return ActionT::INode(a().inode, inode_handler);
}

template <typename ActionT, typename INodeHandlerT>
bool Inflight_setattr<ActionT, INodeHandlerT>::write_success_oplog_result() {
  auto open_flags = ActionT::inode_handler_open_flags(inode_handler);
  if (open_flags.has_value()) {
    OpLogResultOpen result;
    result.set_ino(ino);
    result.set_flags(*open_flags);
    result.set_direct_io(false);
    result.set_keep_cache(false);
    result.set_nonseekable(false);
    return write_oplog_result(result);
  }

  OpLogResultAttr result;
  *result.mutable_attr() = a().inode;
  return write_oplog_result(result);
}

template <typename ActionT, typename INodeHandlerT>
ActionT Inflight_setattr<ActionT, INodeHandlerT>::oplog_recovery(
    const OpLogRecord &record) {
  switch (record.result_case()) {
  case OpLogRecord::kAttr: {
    if (!record.attr().has_attr()) {
      return ActionT::Abort(EIO);
    }
    return ActionT::INode(record.attr().attr(), inode_handler);
  }
  case OpLogRecord::kOpen: {
    INodeRecord inode;
    inode.set_inode(record.open().ino());
    return ActionT::INode(inode, ActionT::inode_handler_open(
                                     static_cast<int>(record.open().flags())));
  }
  default:
    return ActionT::Abort(EIO);
  }
}

template <typename ActionT, typename INodeHandlerT>
ActionT Inflight_setattr<ActionT, INodeHandlerT>::partial_block_fixup() {
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

  return commit(
      std::bind(&Inflight_setattr<ActionT, INodeHandlerT>::commit_cb, this));
}

template <typename ActionT, typename INodeHandlerT>
ActionT Inflight_setattr<ActionT, INodeHandlerT>::callback() {
  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_error_t err;

  err = fdb_future_get_keyvalue_array(a().inode_fetch.get(), &kvs, &kvcount,
                                      &more);
  if (err)
    return ActionT::FDBError(err);
  if (kvcount < 1) {
    if (more) {
      // probably shouldn't be possible, but i'm paranoid about it
      // better to throw an error than falsely report ENOENT
      return ActionT::Abort(EIO);
    } else {
    return ActionT::Abort(ENOENT);
    }
  }

  // sanity check that the first key is the inode.
  const auto expected_key = pack_inode_key(ino);
  if ((kvs[0].key_length != static_cast<int>(expected_key.size())) ||
      (std::memcmp(kvs[0].key, expected_key.data(), expected_key.size()) !=
       0)) {
    return ActionT::Abort(EIO);
  }

  bool do_commit = true;
  auto next_action = ActionT::BeginWait(
      std::bind(&Inflight_setattr<ActionT, INodeHandlerT>::commit_cb, this));

  a().inode.ParseFromArray(kvs[0].value, kvs[0].value_length);
  if (!a().inode.IsInitialized()) {
    // bad inode
    return ActionT::Abort(EIO);
  }

  if (auto it = apply_newer_inode_time_fields(kvs + 1, kvcount - 1, a().inode);
      it.has_value()) {
    // we've integrated any values that were present there, so we can
    // clear them out, now.
    fdbfs_transaction_clear_range(
        transaction.get(), std::make_pair(pack_inode_field_key(ino, {'t'}),
                                          pack_inode_field_key(ino, {'u'})));
  } else {
    return ActionT::Abort(it.error());
  }

  // update inode!

  bool substantial_update = false, data_modification = false;
  if (to_set.has(SetAttrBit::Mode)) {
    a().inode.set_mode(attr.st_mode & 07777);
    substantial_update = true;
  }
  if (to_set.has(SetAttrBit::Uid)) {
    a().inode.set_uid(attr.st_uid);
    substantial_update = true;
  }
  if (to_set.has(SetAttrBit::Gid)) {
    a().inode.set_gid(attr.st_gid);
    substantial_update = true;
  }
  if (to_set.has(SetAttrBit::Size)) {
    if (a().inode.type() != ft_regular) {
      return ActionT::Abort(EINVAL);
    }
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
        next_action = ActionT::BeginWait(std::bind(
            &Inflight_setattr<ActionT, INodeHandlerT>::partial_block_fixup,
            this));
      }
    }
    if (static_cast<uint64_t>(attr.st_size) != a().inode.size()) {
      a().inode.set_size(attr.st_size);
      substantial_update = true;
      data_modification = true;
    }
  }
  if (to_set.has(SetAttrBit::Atime)) {
    a().inode.mutable_atime()->set_sec(attr.st_atim.tv_sec);
    a().inode.mutable_atime()->set_nsec(attr.st_atim.tv_nsec);
  }
  if (to_set.has(SetAttrBit::Mtime)) {
    a().inode.mutable_mtime()->set_sec(attr.st_mtim.tv_sec);
    a().inode.mutable_mtime()->set_nsec(attr.st_mtim.tv_nsec);
  }
  if (to_set.has(SetAttrBit::Ctime)) {
    a().inode.mutable_ctime()->set_sec(attr.st_ctim.tv_sec);
    a().inode.mutable_ctime()->set_nsec(attr.st_ctim.tv_nsec);
  }

  if (to_set.has(SetAttrBit::AtimeNow) || to_set.has(SetAttrBit::MtimeNow) ||
      substantial_update) {
    struct timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    if (to_set.has(SetAttrBit::AtimeNow)) {
      a().inode.mutable_atime()->set_sec(tp.tv_sec);
      a().inode.mutable_atime()->set_nsec(tp.tv_nsec);
    }
    if (to_set.has(SetAttrBit::MtimeNow) || data_modification) {
      a().inode.mutable_mtime()->set_sec(tp.tv_sec);
      a().inode.mutable_mtime()->set_nsec(tp.tv_nsec);
    }
    if (substantial_update) {
      a().inode.mutable_ctime()->set_sec(tp.tv_sec);
      a().inode.mutable_ctime()->set_nsec(tp.tv_nsec);
    }
  }

  if (substantial_update && !to_set.has(SetAttrBit::Mode) &&
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

template <typename ActionT, typename INodeHandlerT>
InflightCallbackT<ActionT> Inflight_setattr<ActionT, INodeHandlerT>::issue() {
  const auto key = pack_inode_key(ino);

  // limit is set to 4 keys, because that's the most we can expect at the
  // moment, inode + {a,m,c}time
  const auto [start_key, stop_key] = pack_inode_and_fields_range(ino);
  wait_on_future(
      fdb_transaction_get_range(
          transaction.get(),
          FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start_key.data(), start_key.size()),
          FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(stop_key.data(), stop_key.size()),
          4, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
      a().inode_fetch);

  return std::bind(&Inflight_setattr<ActionT, INodeHandlerT>::callback, this);
}
