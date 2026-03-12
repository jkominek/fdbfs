
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/xattr.h>

#include "inflight.h"
#include "util.h"

/*************************************************************
 * listxattr
 *************************************************************
 * INITIAL PLAN
 * this is complicated because it has a mechanism for returning
 * the listing, and another mechanism for figuring out how big
 * the listing will be.
 *
 * What's with all the "key_length - empty_xattr_name_length" stuff?
 * The keys are of the form concat(PREFIX, xattr_name).
 * key_length = len(concat(PREFIX, xattr_name)) and
 * empty_xattr_name_length = len(concat(PREFIX, ""))
 * So subtracting that off gets the length of xattr_name, which is
 * what we're interested in.
 *
 * Why do we compute empty_xattr_name_length at the start of every
 * transaction? It depends on the prefix/subspace for the filesystem
 * we're operating on. It could be determined just once at run time,
 * but we haven't implemented a place for all of the do-once-at-run-time
 * precomputations.
 *
 * REAL PLAN
 * ???
 */
template <typename ActionT>
struct AttemptState_listxattr : public AttemptStateT<ActionT> {
  int empty_xattr_name_length = 0;
  std::vector<uint8_t> buf;
  unique_future range_fetch;
};

template <typename ActionT>
class Inflight_listxattr
    : public InflightWithAttemptT<AttemptState_listxattr<ActionT>,
                                  InflightPolicyReadOnly, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_listxattr<ActionT>,
                                    InflightPolicyReadOnly, ActionT>;
  using Base::a;
  using Base::transaction;
  using Base::wait_on_future;

  Inflight_listxattr(fuse_req_t, fdbfs_ino_t, size_t, unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  const fdbfs_ino_t ino;
  const size_t maxsize;
  ActionT process();
};

template <typename ActionT>
Inflight_listxattr<ActionT>::Inflight_listxattr(fuse_req_t req, fdbfs_ino_t ino,
                                                size_t maxsize,
                                                unique_transaction transaction)
    : Base(req, std::move(transaction)),
      ino(ino), maxsize(maxsize) {
  a().buf.reserve(maxsize);
}

template <typename ActionT>
ActionT Inflight_listxattr<ActionT>::process() {
  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_error_t err;

  err = fdb_future_get_keyvalue_array(a().range_fetch.get(), &kvs, &kvcount,
                                      &more);
  if (err)
    return ActionT::FDBError(err);

  // in both of these, we'd ideally immediately check 'more' to see
  // if we need to request additional data, and do so before spending
  // time processing things. but that would risk overwriting the future
  // that we're still pulling things out of. so...
  // TODO make the code more clever, and do this the better way

  for (int i = 0; i < kvcount; i++) {
    const FDBKeyValue *kv = kvs + i;

    int remaining_length = kv->key_length - a().empty_xattr_name_length;
    if (a().buf.size() + remaining_length + 1 > maxsize) {
      // they didn't provide us with a large enough buffer
      return ActionT::Abort(ERANGE);
    }

    auto k = kv->key + a().empty_xattr_name_length;
    a().buf.insert(a().buf.end(), k, k + remaining_length);
    a().buf.push_back(0);
  }

  if ((maxsize <= a().buf.size()) || (!more)) {
    // return the buffer
    return ActionT::Buf(a().buf);
  }

  const FDBKeyValue *lastkv = kvs + (kvcount - 1);
  const auto stop = pack_xattr_node_subspace_range(ino).second;

  // apparently, there is more, and we've got space
  wait_on_future(fdb_transaction_get_range(
                     transaction.get(), lastkv->key, lastkv->key_length, 0, 1,
                     stop.data(), stop.size(), 0, 1, maxsize - a().buf.size(),
                     0,
                     FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
                 a().range_fetch);
  return ActionT::BeginWait(
      std::bind(&Inflight_listxattr<ActionT>::process, this));
}

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_listxattr<ActionT>::issue() {
  const auto [start, stop] = pack_xattr_node_subspace_range(ino);
  a().buf.clear();
  a().buf.reserve(maxsize);

  wait_on_future(
      fdb_transaction_get_range(transaction.get(), start.data(), start.size(),
                                0, 1, stop.data(), stop.size(), 0, 1, maxsize,
                                0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
      a().range_fetch);

  a().empty_xattr_name_length = start.size();

  return std::bind(&Inflight_listxattr<ActionT>::process, this);
}

template <typename ActionT>
struct AttemptState_listxattr_count : public AttemptStateT<ActionT> {
  int empty_xattr_name_length = 0;
  ssize_t accumulated_size = 0;
  unique_future range_fetch;
};

template <typename ActionT>
class Inflight_listxattr_count
    : public InflightWithAttemptT<AttemptState_listxattr_count<ActionT>,
                                  InflightPolicyReadOnly, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_listxattr_count<ActionT>,
                                    InflightPolicyReadOnly, ActionT>;
  using Base::a;
  using Base::transaction;
  using Base::wait_on_future;

  Inflight_listxattr_count(fuse_req_t, fdbfs_ino_t, unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  const fdbfs_ino_t ino;
  ActionT process();
};

template <typename ActionT>
Inflight_listxattr_count<ActionT>::Inflight_listxattr_count(
    fuse_req_t req, fdbfs_ino_t ino, unique_transaction transaction)
    : Base(req, std::move(transaction)),
      ino(ino) {}

template <typename ActionT>
ActionT Inflight_listxattr_count<ActionT>::process() {
  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_error_t err;

  err = fdb_future_get_keyvalue_array(a().range_fetch.get(), &kvs, &kvcount,
                                      &more);
  if (err)
    return ActionT::FDBError(err);

  for (int i = 0; i < kvcount; i++) {
    const FDBKeyValue *kv = kvs + i;
    a().accumulated_size +=
        (kv->key_length - a().empty_xattr_name_length + 1);
  }

  // kvcount>0 check is to make kvcount-1 safe.
  // i don't think it would be possible for more to
  // be true, but we received 0 KV pairs. if that can
  // happen, revisit this. (sounds like it'd be the
  // result of some degenerate situation in fdb, and
  // we'd effectively be reissuing the initial fdb_transaction_get_range)
  if (more && (kvcount > 0)) {
    const FDBKeyValue *lastkv = kvs + (kvcount - 1);
    const auto stop = pack_xattr_node_subspace_range(ino).second;
    wait_on_future(fdb_transaction_get_range(
                       transaction.get(), lastkv->key, lastkv->key_length, 0, 1,
                       stop.data(), stop.size(), 0, 1, 0, 0,
                       FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
                   a().range_fetch);
    return ActionT::BeginWait(
        std::bind(&Inflight_listxattr_count<ActionT>::process, this));
  } else {
    return ActionT::XattrSize(a().accumulated_size);
  }
}

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_listxattr_count<ActionT>::issue() {
  const auto [start, stop] = pack_xattr_node_subspace_range(ino);
  a().accumulated_size = 0;

  wait_on_future(
      fdb_transaction_get_range(transaction.get(), start.data(), start.size(),
                                0, 1, stop.data(), stop.size(), 0, 1, 0, 0,
                                FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
      a().range_fetch);

  a().empty_xattr_name_length = start.size();

  return std::bind(&Inflight_listxattr_count<ActionT>::process, this);
}
