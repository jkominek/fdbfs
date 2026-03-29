
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "inflight.h"
#include "util.h"
#include "util_unlink.h"
#include "values.pb.h"

/*************************************************************
 * unlink
 *************************************************************
 * INITIAL PLAN
 * got an inode for the directory, and the name. need to look
 * up probably a bunch of things, confirm there's permission
 * for the unlink(???) and that the thing in question can be
 * unlinked. if so, remove the dirent, reduce link count.
 *
 * link count at zero:
 * if use count is zero, clearrange the whole thing.
 * otherwise, put on the GC list to be checked asynchronously.
 *
 * REAL PLAN
 * ???
 */
enum class Op { Unlink, Rmdir };

template <typename ActionT>
struct AttemptState_unlink_rmdir : public AttemptStateT<ActionT> {
  // parent inode, for perms checking
  unique_future parent_lookup;
  // for fetching the dirent given parent inode and path name
  unique_future dirent_lookup;
  // fetches the inode record
  unique_future inode_lookup;
  // fetches 0-1 use records
  unique_future inode_use_fetch;
  // fetches 0-1 of the directory entries in a directory
  unique_future directory_listing_fetch;
  // inode of the thing we're removing
  fdbfs_ino_t ino = 0;
};

template <typename ActionT>
class Inflight_unlink_rmdir
    : public InflightWithAttemptT<AttemptState_unlink_rmdir<ActionT>,
                                  InflightPolicyWrite, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_unlink_rmdir<ActionT>,
                                    InflightPolicyWrite, ActionT>;
  using req_t = typename ActionT::req_t;
  using Base::a;
  using Base::commit;
  using Base::track_inode_for_fsync;
  using Base::transaction;
  using Base::wait_on_future;
  using Base::write_oplog_result;

  Inflight_unlink_rmdir(req_t, fdbfs_ino_t, std::string, Op,
                        unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  ActionT postlookup();
  ActionT inode_check();
  ActionT rmdir_inode_dirlist_check();
  ActionT oplog_recovery(const OpLogRecord &) override;
  bool write_success_oplog_result();

  // parent directory
  const fdbfs_ino_t parent;
  // provided name and length
  const std::string name;
  // computed key of the dirent.
  std::vector<uint8_t> dirent_key;
  // how we were invoked, rmdir or unlink
  const Op op;
};

template <typename ActionT>
Inflight_unlink_rmdir<ActionT>::Inflight_unlink_rmdir(
    req_t req, fdbfs_ino_t parent, std::string name, Op op,
    unique_transaction transaction)
    : Base(req, std::move(transaction)), parent(parent), name(std::move(name)),
      op(op) {
  track_inode_for_fsync(parent);
}

template <typename ActionT>
bool Inflight_unlink_rmdir<ActionT>::write_success_oplog_result() {
  OpLogResultOK result;
  return write_oplog_result(result);
}

template <typename ActionT>
ActionT
Inflight_unlink_rmdir<ActionT>::oplog_recovery(const OpLogRecord &record) {
  if (record.result_case() != OpLogRecord::kOk) {
    return ActionT::Abort(EIO);
  }
  return ActionT::OK();
}

template <typename ActionT>
ActionT Inflight_unlink_rmdir<ActionT>::rmdir_inode_dirlist_check() {
  const auto dir_empty =
      keyvalue_range_is_empty(a().directory_listing_fetch.get());
  if (!dir_empty.has_value()) {
    return ActionT::FDBError(dir_empty.error());
  }
  if (!dir_empty.value()) {
    // can't rmdir a directory with any amount of stuff in it.
    return ActionT::Abort(ENOTEMPTY);
  }

  // TODO check the metadata for permission to erase

  const auto parsed = parse_unlink_target_inode(
      a().inode_lookup.get(), a().inode_use_fetch.get(), a().ino);
  if (!parsed.has_value()) {
    if (parsed.error().err != EIO) {
      return ActionT::FDBError(parsed.error().err);
    }
    return ActionT::Abort(parsed.error().err, parsed.error().why);
  }

  INodeRecord inode = parsed->inode;
  track_inode_for_fsync(inode.inode());
  // dirent deletion (has to wait until we're sure we can remove the
  // entire thing)
  fdb_transaction_clear(transaction.get(), dirent_key.data(),
                        dirent_key.size());

  const auto mutation_result = apply_unlink_target_mutation(
      transaction.get(), inode, parsed->inode_in_use,
      UnlinkApplyOptions{
          .nlink_mutation = UnlinkNlinkMutation::SetZero,
          .unlink_directory_semantics = false,
      });
  if (!mutation_result.has_value()) {
    return ActionT::Abort(mutation_result.error());
  }

  if (!write_success_oplog_result()) {
    return ActionT::Abort(EIO);
  }

  return commit(ActionT::OK);
}

template <typename ActionT>
ActionT Inflight_unlink_rmdir<ActionT>::inode_check() {
  // TODO check the metadata for permission to erase

  const auto parsed = parse_unlink_target_inode(
      a().inode_lookup.get(), a().inode_use_fetch.get(), a().ino);
  if (!parsed.has_value()) {
    if (parsed.error().err != EIO) {
      return ActionT::FDBError(parsed.error().err);
    }
    return ActionT::Abort(parsed.error().err, parsed.error().why);
  }
  INodeRecord inode = parsed->inode;
  track_inode_for_fsync(inode.inode());

  const auto mutation_result = apply_unlink_target_mutation(
      transaction.get(), inode, parsed->inode_in_use,
      UnlinkApplyOptions{
          .nlink_mutation = UnlinkNlinkMutation::Decrement,
          .unlink_directory_semantics = false,
      });
  if (!mutation_result.has_value()) {
    return ActionT::Abort(mutation_result.error());
  }

  if (!write_success_oplog_result()) {
    return ActionT::Abort(EIO);
  }

  return commit(ActionT::OK);
}

template <typename ActionT>
ActionT Inflight_unlink_rmdir<ActionT>::postlookup() {
  fdb_bool_t dirinode_present, dirent_present;
  const uint8_t *value;
  int valuelen;
  fdb_error_t err;

  err = fdb_future_get_value(a().parent_lookup.get(), &dirinode_present, &value,
                             &valuelen);
  if (err)
    return ActionT::FDBError(err);

  if (!dirinode_present) {
    return ActionT::Abort(ENOENT);
  }

  INodeRecord parent;
  parent.ParseFromArray(value, valuelen);
  if (!(parent.IsInitialized() && parent.has_nlinks())) {
    return ActionT::Abort(EIO);
  }
  if (op == Op::Rmdir) {
    if (parent.nlinks() == 0) {
      return ActionT::Abort(EIO);
    }
    parent.set_nlinks(parent.nlinks() - 1);
  }
  if (auto it = update_directory_times(
          transaction.get(), parent,
          op == Op::Rmdir ? DirectoryUpdateKind::ContentsPersist
                          : DirectoryUpdateKind::ContentsDeferred);
      !it.has_value()) {
    return ActionT::FDBError(it.error());
  }

  err = fdb_future_get_value(a().dirent_lookup.get(), &dirent_present, &value,
                             &valuelen);
  if (err)
    return ActionT::FDBError(err);

  if (!dirent_present) {
    return ActionT::Abort(ENOENT);
  }

  filetype dirent_type;

  {
    DirectoryEntry dirent;
    dirent.ParseFromArray(value, valuelen);
    if (!dirent.IsInitialized()) {
      // bad record,
      return ActionT::Abort(EIO);
    }

    a().ino = dirent.inode();
    dirent_type = dirent.type();
  }

  // check the values in the dirent to make sure
  // we're looking at the right kind of thing. bail
  // if it isn't the right thing.
  if (op == Op::Rmdir) {
    // we want to find a directory
    if (dirent_type == ft_directory) {
      // ok, we've successfully found something rmdir'able.

      // can't remove the dirent here, though, as there might be
      // dirents in the directory.

      {
        const auto key = pack_inode_key(a().ino);
        wait_on_future(
            fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
            a().inode_lookup);
      }

      {
        const auto [start, stop] = pack_inode_use_subspace_range(a().ino);

        wait_on_future(
            fdb_transaction_get_range(
                transaction.get(),
                FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start.data(), start.size()),
                FDB_KEYSEL_FIRST_GREATER_THAN(stop.data(), stop.size()), 1, 0,
                FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
            a().inode_use_fetch);
      }

      // we want to scan for any directory entries inside of this
      // directory. so we'll produce a key from before the first
      // possible directory entry, and one for after the last
      // possible, and then get the range, limit 1.
      {
        const auto [start, stop] = pack_dentry_subspace_range(a().ino);

        wait_on_future(
            fdb_transaction_get_range(
                transaction.get(),
                FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start.data(), start.size()),
                FDB_KEYSEL_FIRST_GREATER_THAN(stop.data(), stop.size()), 1, 0,
                FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
            a().directory_listing_fetch);
      }

      return ActionT::BeginWait(std::bind(
          &Inflight_unlink_rmdir<ActionT>::rmdir_inode_dirlist_check, this));
    } else {
      // mismatch. bail.
      return ActionT::Abort(ENOTDIR);
    }
  } else {
    // we want anything except a directory
    if (dirent_type != S_IFDIR) {
      // successfully found something unlinkable.
      fdb_transaction_clear(transaction.get(), dirent_key.data(),
                            dirent_key.size());

      {
        const auto key = pack_inode_key(a().ino);
        wait_on_future(
            fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
            a().inode_lookup);
      }

      {
        const auto [start, stop] = pack_inode_use_subspace_range(a().ino);

        // we'll use this to check if there are any use records at all.
        wait_on_future(
            fdb_transaction_get_range(
                transaction.get(),
                FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start.data(), start.size()),
                FDB_KEYSEL_FIRST_GREATER_THAN(stop.data(), stop.size()), 1, 0,
                FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
            a().inode_use_fetch);
      }
      return ActionT::BeginWait(
          std::bind(&Inflight_unlink_rmdir<ActionT>::inode_check, this));
    } else {
      // mismatch. bail.
      return ActionT::Abort(EISDIR);
    }
  }
}

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_unlink_rmdir<ActionT>::issue() {
  const auto name_kind = classify_dentry_name(name);
  if (name_kind != DentryNameKind::Normal) {
    if (op == Op::Unlink) {
      return []() { return ActionT::Abort(EISDIR); };
    } else if (name_kind == DentryNameKind::Self) {
      return []() { return ActionT::Abort(EINVAL); };
    } else if (name_kind == DentryNameKind::Parent) {
      return []() { return ActionT::Abort(ENOTEMPTY); };
    }
    std::unreachable();
  }

  dirent_key = pack_dentry_key(parent, name);

  // fetch parent inode so we can check permissions
  const auto key = pack_inode_key(parent);
  wait_on_future(
      fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
      a().parent_lookup);

  // fetch the dirent so we can get the inode info.
  wait_on_future(fdb_transaction_get(transaction.get(), dirent_key.data(),
                                     dirent_key.size(), 0),
                 a().dirent_lookup);
  return std::bind(&Inflight_unlink_rmdir<ActionT>::postlookup, this);
}
