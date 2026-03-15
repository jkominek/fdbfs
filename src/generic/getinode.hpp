
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <iostream>

#include "inflight.h"
#include "util.h"

#include <cstdint>
#include <typeinfo>

/*************************************************************
 * getattr
 *************************************************************
 * INITIAL PLAN
 * this should be a single get.
 *
 * REAL PLAN
 * maybe a small range read to pick up extended attributes
 * or less common values?
 */
template <typename ActionT>
struct AttemptState_getinode : public AttemptStateT<ActionT> {
  unique_future inode_fetch;
};

template <typename ActionT, typename INodeHandlerT>
class Inflight_getinode
    : public InflightWithAttemptT<AttemptState_getinode<ActionT>, InflightPolicyReadOnly,
                                  ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_getinode<ActionT>,
                                    InflightPolicyReadOnly, ActionT>;
  using req_t = typename ActionT::req_t;
  using Base::a;
  using Base::transaction;
  using Base::wait_on_future;

  Inflight_getinode(req_t, fdbfs_ino_t, unique_transaction, INodeHandlerT);
  InflightCallbackT<ActionT> issue();

private:
  const fdbfs_ino_t ino;
  const INodeHandlerT inode_handler;
  ActionT callback();
};

template <typename ActionT, typename INodeHandlerT>
Inflight_getinode<ActionT, INodeHandlerT>::Inflight_getinode(
    req_t req, fdbfs_ino_t ino, unique_transaction transaction,
    INodeHandlerT inode_handler)
    : Base(req, std::move(transaction)), ino(ino), inode_handler(inode_handler) {}

template <typename ActionT, typename INodeHandlerT>
ActionT Inflight_getinode<ActionT, INodeHandlerT>::callback() {
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

  INodeRecord inode;
  inode.ParseFromArray(val, vallen);
  if (!inode.IsInitialized()) {
    return ActionT::Abort(EIO);
  }

  return ActionT::INode(inode, inode_handler);
}

template <typename ActionT, typename INodeHandlerT>
InflightCallbackT<ActionT> Inflight_getinode<ActionT, INodeHandlerT>::issue() {
  auto key = pack_inode_key(ino);

  // and request just that inode
  wait_on_future(
      fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
      a().inode_fetch);
  return std::bind(&Inflight_getinode<ActionT, INodeHandlerT>::callback, this);
}
