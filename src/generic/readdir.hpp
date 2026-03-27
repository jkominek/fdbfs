
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <string_view>

#include "inflight.h"
#include "util.h"
#include "values.pb.h"

/*************************************************************
 * readdir
 *************************************************************
 * INITIAL PLAN
 * ?
 *
 * REAL PLAN
 * ?
 */

template <typename ActionT>
struct AttemptState_readdir : public AttemptStateT<ActionT> {
  unique_future range_fetch;
};

template <typename ActionT>
class Inflight_readdir
    : public InflightWithAttemptT<AttemptState_readdir<ActionT>,
                                  InflightPolicyReadOnly, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_readdir<ActionT>,
                                    InflightPolicyReadOnly, ActionT>;
  using req_t = typename ActionT::req_t;
  using Base::a;
  using Base::req;
  using Base::transaction;
  using Base::wait_on_future;

  Inflight_readdir(req_t, fdbfs_ino_t, typename ActionT::DirentCollectorSpec,
                   std::string_view, unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  const fdbfs_ino_t ino;
  const typename ActionT::DirentCollectorSpec collector_spec;
  const std::string start_name;

  ActionT callback();
};

template <typename ActionT>
Inflight_readdir<ActionT>::Inflight_readdir(
    req_t req, fdbfs_ino_t ino,
    typename ActionT::DirentCollectorSpec collector_spec,
    std::string_view start_name, unique_transaction transaction)
    : Base(req, std::move(transaction)), ino(ino),
      collector_spec(std::move(collector_spec)), start_name(start_name) {}

template <typename ActionT> ActionT Inflight_readdir<ActionT>::callback() {
  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_error_t err;

  err = fdb_future_get_keyvalue_array(a().range_fetch.get(), &kvs, &kvcount,
                                      &more);
  if (err)
    return ActionT::FDBError(err);

  auto collector = ActionT::make_dirent_collector(req, collector_spec);

  for (int i = 0; i < kvcount; i++) {
    FDBKeyValue kv = kvs[i];

    if (kv.key_length <= dirent_prefix_length) {
      // serious internal error. we somehow got back a key that was too short?
      return ActionT::Abort(EIO);
    }
    int keylen = kv.key_length - dirent_prefix_length;
    if ((keylen <= 0) || (keylen > MAXFILENAMELEN)) {
      // internal error
      return ActionT::Abort(EIO);
    }
    std::string_view name(
        reinterpret_cast<const char *>(kv.key) + dirent_prefix_length, keylen);

    DirectoryEntry dirent;
    dirent.ParseFromArray(kv.value, kv.value_length);

    if (!dirent.IsInitialized()) {
      return ActionT::Abort(EIO);
    }

    if (!collector.try_add(name, dirent).has_value()) {
      // ran out of space. last one failed. we're done.
      break;
    }
  }

  return std::move(collector).finish();
}

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_readdir<ActionT>::issue() {
  auto [start_key, stop_key] = pack_dentry_subspace_range(ino);
  int begin_or_equal = 0;
  int begin_offset = 1;
  if (!start_name.empty()) {
    start_key = pack_dentry_key(ino, start_name);
    begin_or_equal = 1;
    begin_offset = 1;
  }

  auto collector = ActionT::make_dirent_collector(req, collector_spec);
  const size_t estimated_count = collector.estimate_remaining_entries();
  int limit = static_cast<int>(estimated_count);
  limit = std::max<size_t>(8, limit);

  wait_on_future(
      fdb_transaction_get_range(transaction.get(), start_key.data(),
                                start_key.size(), begin_or_equal, begin_offset,
                                stop_key.data(), stop_key.size(), 0, 1, limit,
                                0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
      a().range_fetch);
  return std::bind(&Inflight_readdir<ActionT>::callback, this);
}
