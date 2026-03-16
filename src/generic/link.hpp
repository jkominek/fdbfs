#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "inflight.h"
#include "util.h"
#include "values.pb.h"

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
template <typename ActionT>
struct AttemptState_link : public AttemptStateT<ActionT> {
  INodeRecord inode;
  unique_future file_lookup;
  unique_future dir_lookup;
  unique_future target_lookup;
};

template <typename ActionT, typename INodeHandlerT>
class Inflight_link
    : public InflightWithAttemptT<AttemptState_link<ActionT>,
                                  InflightPolicyWrite, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_link<ActionT>,
                                    InflightPolicyWrite, ActionT>;
  using req_t = typename ActionT::req_t;
  using Base::a;
  using Base::commit;
  using Base::track_inode_for_fsync;
  using Base::transaction;
  using Base::wait_on_future;
  using Base::write_oplog_result;

  Inflight_link(req_t, fdbfs_ino_t, fdbfs_ino_t, std::string,
                unique_transaction, INodeHandlerT);
  InflightCallbackT<ActionT> issue();

private:
  const fdbfs_ino_t ino;
  const fdbfs_ino_t newparent;
  const std::string newname;
  const INodeHandlerT inode_handler;

  ActionT check();
  ActionT oplog_recovery(const OpLogRecord &) override;
  bool write_success_oplog_result();
};

template <typename ActionT, typename INodeHandlerT>
Inflight_link<ActionT, INodeHandlerT>::Inflight_link(
    req_t req, fdbfs_ino_t ino, fdbfs_ino_t newparent, std::string newname,
    unique_transaction transaction, INodeHandlerT inode_handler)
    : Base(req, std::move(transaction)), ino(ino), newparent(newparent),
      newname(std::move(newname)), inode_handler(std::move(inode_handler)) {
  track_inode_for_fsync(ino);
  track_inode_for_fsync(newparent);
}

template <typename ActionT, typename INodeHandlerT>
bool Inflight_link<ActionT, INodeHandlerT>::write_success_oplog_result() {
  OpLogResultEntry result;
  result.set_ino(ino);
  result.set_generation(1);
  *result.mutable_attr() = a().inode;
  result.set_attr_timeout(0.01);
  result.set_entry_timeout(0.01);
  return write_oplog_result(result);
}

template <typename ActionT, typename INodeHandlerT>
ActionT Inflight_link<ActionT, INodeHandlerT>::oplog_recovery(
    const OpLogRecord &record) {
  if (record.result_case() != OpLogRecord::kEntry) {
    return ActionT::Abort(EIO);
  }
  if (!record.entry().has_attr()) {
    return ActionT::Abort(EIO);
  }
  return ActionT::INode(record.entry().attr(), inode_handler);
}

template <typename ActionT, typename INodeHandlerT>
ActionT Inflight_link<ActionT, INodeHandlerT>::check() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;

  // is the file a non-directory?
  err = fdb_future_get_value(a().file_lookup.get(), &present, &val, &vallen);
  if (err)
    return ActionT::FDBError(err);
  if (present) {
    a().inode.ParseFromArray(val, vallen);
    if (!a().inode.has_type()) {
      // error
      return ActionT::Abort(EIO);
    } else if (a().inode.type() == ft_directory) {
      // can hardlink anything except a directory
      return ActionT::Abort(EPERM);
    }
    // we could lift this value and save it for the
    // other dirent we need to create?
  } else {
    // apparently it isn't there. sad.
    return ActionT::Abort(ENOENT);
  }

  // is the directory a directory?
  err = fdb_future_get_value(a().dir_lookup.get(), &present, &val, &vallen);
  if (err)
    return ActionT::FDBError(err);
  if (present) {
    INodeRecord dirinode;
    dirinode.ParseFromArray(val, vallen);
    if (!dirinode.has_type()) {
      return ActionT::Abort(EIO);
      // error
    }
    if (dirinode.type() != ft_directory) {
      // have to hardlink into a directory
      return ActionT::Abort(ENOTDIR);
    }
    // update times on destination dir
    if (auto it = update_directory_times(transaction.get(), dirinode);
        !it.has_value()) {
      return ActionT::FDBError(it.error());
    }
  } else {
    return ActionT::Abort(ENOENT);
  }

  // Does the target exist?
  err = fdb_future_get_value(a().target_lookup.get(), &present, &val, &vallen);
  if (err)
    return ActionT::FDBError(err);
  if (present) {
    // that's an error. :(
    return ActionT::Abort(EEXIST);
  }

  // need to update the inode attributes
  a().inode.set_nlinks(a().inode.nlinks() + 1);
  struct timespec tv;
  clock_gettime(CLOCK_REALTIME, &tv);
  update_ctime(&a().inode, &tv);

  // TODO do we need to touch any other inode attributes when
  // creating a hard link?

  if (!fdb_set_protobuf(transaction.get(), pack_inode_key(ino), a().inode))
    return ActionT::Abort(EIO);

  // also need to add the new directory entry
  DirectoryEntry dirent;
  dirent.set_inode(ino);
  dirent.set_type(a().inode.type());

  if (!fdb_set_protobuf(transaction.get(), pack_dentry_key(newparent, newname),
                        dirent))
    return ActionT::Abort(EIO);

  if (!write_success_oplog_result()) {
    return ActionT::Abort(EIO);
  }

  auto *inode_ptr = &(a().inode);
  return commit([inode_ptr, inode_handler = inode_handler]() {
    return ActionT::INode(*inode_ptr, inode_handler);
  });
}

template <typename ActionT, typename INodeHandlerT>
InflightCallbackT<ActionT> Inflight_link<ActionT, INodeHandlerT>::issue() {
  // check that the file is a file
  {
    const auto key = pack_inode_key(ino);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        a().file_lookup);
  }

  // check/update destination directory
  {
    const auto key = pack_inode_key(newparent);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        a().dir_lookup);
  }

  // check nothing exists in the destination
  {
    const auto key = pack_dentry_key(newparent, newname);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        a().target_lookup);
  }

  return std::bind(&Inflight_link<ActionT, INodeHandlerT>::check, this);
}
