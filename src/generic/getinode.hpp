
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
    : public InflightWithAttemptT<AttemptState_getinode<ActionT>,
                                  InflightPolicyReadOnly, ActionT> {
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
    : Base(req, std::move(transaction)), ino(ino),
      inode_handler(inode_handler) {}

template <typename ActionT, typename INodeHandlerT>
ActionT Inflight_getinode<ActionT, INodeHandlerT>::callback() {
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
      return ActionT::Abort(EIO);
    } else {
      return ActionT::Abort(ENOENT);
    }
  }

  const auto expected_key = pack_inode_key(ino);
  if ((kvs[0].key_length != static_cast<int>(expected_key.size())) ||
      (std::memcmp(kvs[0].key, expected_key.data(), expected_key.size()) !=
       0)) {
    return ActionT::Abort(EIO);
  }

  INodeRecord inode;
  inode.ParseFromArray(kvs[0].value, kvs[0].value_length);
  if (!inode.IsInitialized()) {
    return ActionT::Abort(EIO);
  }

  if (auto it = apply_newer_inode_time_fields(kvs + 1, kvcount - 1, inode);
      !it.has_value()) {
    return ActionT::Abort(it.error());
  }

  return ActionT::INode(inode, inode_handler);
}

template <typename ActionT, typename INodeHandlerT>
InflightCallbackT<ActionT> Inflight_getinode<ActionT, INodeHandlerT>::issue() {
  const auto [start_key, stop_key] = pack_inode_and_fields_range(ino);
  wait_on_future(
      fdb_transaction_get_range(
          transaction.get(),
          FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start_key.data(), start_key.size()),
          FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(stop_key.data(), stop_key.size()),
          4, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
      a().inode_fetch);
  return std::bind(&Inflight_getinode<ActionT, INodeHandlerT>::callback, this);
}
