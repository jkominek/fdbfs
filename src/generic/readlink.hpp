
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

/*************************************************************
 * readlink
 *************************************************************
 * INITIAL PLAN
 * this should be a single get, as we only store symlinks in
 * the inode. we _could_ fall back to storing very large symlinks
 * in the data blocks.
 *
 * REAL PLAN
 * ???
 */
template <typename ActionT>
struct AttemptState_readlink : public AttemptStateT<ActionT> {
  unique_future inode_fetch;
};

template <typename ActionT>
class Inflight_readlink
    : public InflightWithAttemptT<AttemptState_readlink<ActionT>,
                                  InflightPolicyReadOnly, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_readlink<ActionT>,
                                    InflightPolicyReadOnly, ActionT>;
  using Base::a;
  using Base::transaction;
  using Base::wait_on_future;

  Inflight_readlink(fuse_req_t, fuse_ino_t, unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  const fuse_ino_t ino;
  ActionT callback();
};

template <typename ActionT>
Inflight_readlink<ActionT>::Inflight_readlink(fuse_req_t req, fuse_ino_t ino,
                                              unique_transaction transaction)
    : Base(req, std::move(transaction)), ino(ino) {}

template <typename ActionT> ActionT Inflight_readlink<ActionT>::callback() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(a().inode_fetch.get(), &present, &val, &vallen);
  if (err)
    return ActionT::FDBError(err);

  if (present) {
    INodeRecord inode;
    inode.ParseFromArray(val, vallen);
    if (!inode.has_symlink()) {
      if (inode.type() == ft_symlink) {
        return ActionT::Abort(EIO);
      } else {
        return ActionT::Abort(EINVAL);
      }
    }
    return ActionT::Readlink(inode.symlink());
  } else {
    return ActionT::Abort(ENOENT);
  }
}

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_readlink<ActionT>::issue() {
  const auto key = pack_inode_key(ino);

  // and request just that inode
  wait_on_future(
      fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
      a().inode_fetch);
  return std::bind(&Inflight_readlink<ActionT>::callback, this);
}
