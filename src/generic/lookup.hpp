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
 * lookup
 *************************************************************
 * INITIAL PLAN
 * not too bad. given a directory inode and a name, we:
 *   1. fetch the directory entry, getting us an inode.
 *   2. getattr-equiv on that inode.
 *
 * REAL PLAN
 * might be a good spot for an optimization? we can finish off
 * the fuse request, but then maintain some inode cache with
 * the last unchanging attributes of an inode that we've seen.
 * we could reject invalid requests to inodes faster. (readdir
 * on a file, for instance?) is it worth it to make the error
 * case faster?
 *
 * TRANSACTIONAL BEHAVIOR
 * We're doing the two reads as snapshots. Since the filesystem
 * can change arbitrarily immediately after we're done, it doesn't
 * much matter if it changes by a little or a lot. Just want to
 * ensure that we show the user something that was true.
 */
template <typename ActionT>
struct AttemptState_lookup : public AttemptStateT<ActionT> {
  fdbfs_ino_t target = 0;
  unique_future dirent_fetch;
  unique_future inode_fetch;
};

template <typename ActionT, typename INodeHandlerT>
class Inflight_lookup
    : public InflightWithAttemptT<AttemptState_lookup<ActionT>,
                                  InflightPolicyReadOnly, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_lookup<ActionT>,
                                    InflightPolicyReadOnly, ActionT>;
  using req_t = typename ActionT::req_t;
  using Base::a;
  using Base::transaction;
  using Base::wait_on_future;

  Inflight_lookup(req_t, fdbfs_ino_t, std::string, unique_transaction,
                  INodeHandlerT);
  InflightCallbackT<ActionT> issue();

private:
  const fdbfs_ino_t parent;
  const std::string name;
  const INodeHandlerT inode_handler;

  // issue looks up the dirent and then...
  ActionT lookup_inode();
  ActionT process_inode();
};

template <typename ActionT, typename INodeHandlerT>
Inflight_lookup<ActionT, INodeHandlerT>::Inflight_lookup(
    req_t req, fdbfs_ino_t parent, std::string name,
    unique_transaction transaction, INodeHandlerT inode_handler)
    : Base(req, std::move(transaction)), parent(parent), name(std::move(name)),
      inode_handler(std::move(inode_handler)) {}

template <typename ActionT, typename INodeHandlerT>
ActionT Inflight_lookup<ActionT, INodeHandlerT>::process_inode() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(a().inode_fetch.get(), &present, &val, &vallen);
  if (err)
    return ActionT::FDBError(err);

  // and second callback, to get the attributes
  if (!present) {
    return ActionT::Abort(EIO);
  }

  INodeRecord inode;
  inode.ParseFromArray(val, vallen);
  if (!inode.IsInitialized()) {
    return ActionT::Abort(EIO);
  }

  return ActionT::INode(inode, inode_handler);
}

template <typename ActionT, typename INodeHandlerT>
ActionT Inflight_lookup<ActionT, INodeHandlerT>::lookup_inode() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(a().dirent_fetch.get(), &present, &val, &vallen);
  if (err)
    return ActionT::FDBError(err);

  // we're on the first callback, to get the directory entry
  if (present) {
    {
      DirectoryEntry dirent;
      dirent.ParseFromArray(val, vallen);
      if (!dirent.has_inode()) {
        return ActionT::Abort(EIO, "directory entry missing inode");
      }
      a().target = dirent.inode();
    }

    const auto key = pack_inode_key(a().target);

    // and request just that inode
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 1),
        a().inode_fetch);
    return ActionT::BeginWait(
        std::bind(&Inflight_lookup<ActionT, INodeHandlerT>::process_inode, this));
  } else {
    return ActionT::Abort(ENOENT);
  }
}

template <typename ActionT, typename INodeHandlerT>
InflightCallbackT<ActionT> Inflight_lookup<ActionT, INodeHandlerT>::issue() {
  const auto key = pack_dentry_key(parent, name);

  wait_on_future(
      fdb_transaction_get(transaction.get(), key.data(), key.size(), 1),
      a().dirent_fetch);
  return std::bind(&Inflight_lookup<ActionT, INodeHandlerT>::lookup_inode, this);
}
