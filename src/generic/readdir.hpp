
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
  unique_future dirinode_fetch;
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
                   ReaddirStartKind, std::string_view, unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  const fdbfs_ino_t ino;
  const typename ActionT::DirentCollectorSpec collector_spec;
  const ReaddirStartKind start_kind;
  const std::string start_name;

  ActionT callback();
};

template <typename ActionT>
Inflight_readdir<ActionT>::Inflight_readdir(
    req_t req, fdbfs_ino_t ino,
    typename ActionT::DirentCollectorSpec collector_spec,
    ReaddirStartKind start_kind, std::string_view start_name,
    unique_transaction transaction)
    : Base(req, std::move(transaction)), ino(ino),
      collector_spec(std::move(collector_spec)), start_kind(start_kind),
      start_name(start_name) {
  assert((start_kind == ReaddirStartKind::AfterName)
             ? start_name.empty() == false
             : start_name.empty());
}

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

  if (a().dirinode_fetch) {
    fdb_bool_t present = 0;
    const uint8_t *val = nullptr;
    int vallen = 0;

    err =
        fdb_future_get_value(a().dirinode_fetch.get(), &present, &val, &vallen);
    if (err) {
      return ActionT::FDBError(err);
    }
    if (!present) {
      return ActionT::Abort(ENOENT);
    }

    INodeRecord dirinode;
    dirinode.ParseFromArray(val, vallen);
    // this is pretty unlikely!
    if (!(dirinode.IsInitialized() && (dirinode.type() == ft_directory) &&
          dirinode.has_parentinode())) {
      return ActionT::Abort(EIO);
    }

    auto add_synthetic =
        [&](std::string_view name,
            fdbfs_ino_t entry_ino) -> std::expected<void, int> {
      DirectoryEntry dirent;
      dirent.set_inode(entry_ino);
      dirent.set_type(ft_directory);
      if (!collector.try_add(name, dirent).has_value()) {
        return std::unexpected(0);
      }
      return {};
    };

    switch (start_kind) {
    case ReaddirStartKind::Beginning:
      if (!add_synthetic(".", ino).has_value()) {
        return std::move(collector).finish();
      }
      [[fallthrough]];
    case ReaddirStartKind::AfterDot:
      if (!add_synthetic("..", dirinode.parentinode()).has_value()) {
        return std::move(collector).finish();
      }
      break;
    case ReaddirStartKind::AfterDotDot:
    case ReaddirStartKind::AfterName:
      break;
    }
  }

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
  if (start_kind == ReaddirStartKind::AfterName) {
    start_key = pack_dentry_key(ino, start_name);
    begin_or_equal = 1;
    begin_offset = 1;
  }

  auto collector = ActionT::make_dirent_collector(req, collector_spec);
  int estimated_count = collector.estimate_remaining_entries();
  bool fetch_inode = false;
  if (start_kind == ReaddirStartKind::Beginning) {
    estimated_count -= 2;
    fetch_inode = true;
  } else if (start_kind == ReaddirStartKind::AfterDot) {
    estimated_count -= 1;
    fetch_inode = true;
  }
  if (fetch_inode) {
    // if we've got to include ".." in the results, then we'll
    // need our inode, so we can pull the parentinode value out of it.
    const auto key = pack_inode_key(ino);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 1),
        a().dirinode_fetch);
  }
  int limit = std::max(8, estimated_count);

  wait_on_future(
      fdb_transaction_get_range(transaction.get(), start_key.data(),
                                start_key.size(), begin_or_equal, begin_offset,
                                stop_key.data(), stop_key.size(), 0, 1, limit,
                                0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
      a().range_fetch);
  return std::bind(&Inflight_readdir<ActionT>::callback, this);
}
