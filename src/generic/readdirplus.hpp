#include <errno.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "inflight.h"
#include "util.h"
#include "values.pb.h"

/*************************************************************
 * readdirplus
 *************************************************************
 */

struct ReaddirPlusEntry {
  std::string name;
  DirectoryEntry dirent;
};

template <typename ActionT>
struct AttemptState_readdirplus : public AttemptStateT<ActionT> {
  unique_future range_fetch;
  std::vector<ReaddirPlusEntry> entries;
  std::vector<unique_future> inode_fetches;
  std::optional<typename ActionT::DirentCollector> reply_collector;
};

template <typename ActionT>
class Inflight_readdirplus
    : public InflightWithAttemptT<AttemptState_readdirplus<ActionT>,
                                  InflightPolicyReadOnly, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_readdirplus<ActionT>,
                                    InflightPolicyReadOnly, ActionT>;
  using req_t = typename ActionT::req_t;
  using Base::a;
  using Base::commit;
  using Base::req;
  using Base::transaction;
  using Base::wait_on_future;

  Inflight_readdirplus(req_t, fdbfs_ino_t,
                       typename ActionT::DirentCollectorSpec, std::string_view,
                       unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  ActionT dirent_callback();
  ActionT callback();
  ActionT reply_buffer();

  const fdbfs_ino_t ino;
  const typename ActionT::DirentCollectorSpec collector_spec;
  const std::string start_name;
};

template <typename ActionT>
Inflight_readdirplus<ActionT>::Inflight_readdirplus(
    req_t req, fdbfs_ino_t ino,
    typename ActionT::DirentCollectorSpec collector_spec,
    std::string_view start_name, unique_transaction transaction)
    : Base(req, std::move(transaction)), ino(ino),
      collector_spec(std::move(collector_spec)), start_name(start_name) {}

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_readdirplus<ActionT>::issue() {
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
  // limit ourselves to pulling 128 entries at once, since that's a decent
  // amount of potential traffic.
  const int limit = static_cast<int>(
      std::max<size_t>(2, std::min<size_t>(128, estimated_count)));

  wait_on_future(
      fdb_transaction_get_range(transaction.get(), start_key.data(),
                                start_key.size(), begin_or_equal, begin_offset,
                                stop_key.data(), stop_key.size(), 0, 1, limit,
                                0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
      a().range_fetch);
  return std::bind(&Inflight_readdirplus<ActionT>::dirent_callback, this);
}

template <typename ActionT>
ActionT Inflight_readdirplus<ActionT>::dirent_callback() {
  const FDBKeyValue *kvs = nullptr;
  int kvcount = 0;
  fdb_bool_t more = 0;
  const fdb_error_t err = fdb_future_get_keyvalue_array(a().range_fetch.get(),
                                                        &kvs, &kvcount, &more);
  if (err) {
    return ActionT::FDBError(err);
  }
  // if there's more, that's fine, the caller can make another request, and
  // we'll fetch it then.
  (void)more;

  a().entries.clear();
  a().inode_fetches.clear();
  a().entries.reserve(kvcount);
  a().inode_fetches.reserve(kvcount);

  for (int i = 0; i < kvcount; i++) {
    const FDBKeyValue &kv = kvs[i];
    if (kv.key_length <= dirent_prefix_length) {
      return ActionT::Abort(EIO);
    }
    const int keylen = kv.key_length - dirent_prefix_length;
    if (keylen <= 0 || keylen > MAXFILENAMELEN) {
      return ActionT::Abort(EIO);
    }

    DirectoryEntry dirent;
    if (!dirent.ParseFromArray(kv.value, kv.value_length) ||
        !dirent.IsInitialized()) {
      return ActionT::Abort(EIO);
    }

    // TODO now that we have the filenames, we could precisely
    // determine how many of them we can fit into the buffer we
    // were provided, and if that is less than we previously
    // estimated, issue fewer get requests since we know we won't
    // be able to use them

    ReaddirPlusEntry entry{
        .name = std::string(reinterpret_cast<const char *>(kv.key) +
                                dirent_prefix_length,
                            keylen),
        .dirent = dirent,
    };
    a().entries.emplace_back(std::move(entry));

    const auto [start_key, stop_key] =
        pack_inode_and_fields_range(dirent.inode());
    a().inode_fetches.emplace_back();
    wait_on_future(
        fdb_transaction_get_range(
            transaction.get(),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start_key.data(),
                                              start_key.size()),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(stop_key.data(), stop_key.size()),
            4, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
        a().inode_fetches.back());
  }

  if (a().inode_fetches.empty()) {
    auto collector = ActionT::make_dirent_collector(req, collector_spec);
    return std::move(collector).finish();
  }
  return ActionT::BeginWait(
      std::bind(&Inflight_readdirplus<ActionT>::callback, this));
}

template <typename ActionT> ActionT Inflight_readdirplus<ActionT>::callback() {
  if (a().inode_fetches.size() != a().entries.size()) {
    return ActionT::Abort(EIO);
  }

  auto collector = ActionT::make_dirent_collector(req, collector_spec);

  std::vector<std::pair<fdbfs_ino_t, uint64_t>> use_records;
  use_records.reserve(a().entries.size());

  for (size_t i = 0; i < a().entries.size(); i++) {
    const FDBKeyValue *kvs = nullptr;
    int kvcount = 0;
    fdb_bool_t more = 0;
    const fdb_error_t err = fdb_future_get_keyvalue_array(
        a().inode_fetches[i].get(), &kvs, &kvcount, &more);
    if (err) {
      return ActionT::FDBError(err);
    }
    if (kvcount < 1) {
      // this is some sort of db corruption; can we maybe stuff an
      // error into the response, or fill it out partially, so that
      // users can at least see which dirent is misbehaving?
      return ActionT::Abort(EIO);
    }

    const auto expected_key = pack_inode_key(a().entries[i].dirent.inode());
    if ((kvs[0].key_length != static_cast<int>(expected_key.size())) ||
        (std::memcmp(kvs[0].key, expected_key.data(), expected_key.size()) !=
         0)) {
      return ActionT::Abort(EIO);
    }

    INodeRecord inode;
    if (!inode.ParseFromArray(kvs[0].value, kvs[0].value_length) ||
        !inode.IsInitialized()) {
      return ActionT::Abort(EIO);
    }
    if (inode.inode() != a().entries[i].dirent.inode()) {
      return ActionT::Abort(EIO);
    }
    if (auto it = apply_newer_inode_time_fields(kvs + 1, kvcount - 1, inode);
        !it.has_value()) {
      return ActionT::Abort(it.error());
    }

    auto add_result =
        collector.try_add(a().entries[i].name, a().entries[i].dirent, &inode);
    if (!add_result.has_value()) {
      if (add_result.error() == DirentAddError::InvalidInput) {
        return ActionT::Abort(EIO);
      }
      break;
    }

    auto generation = increment_lookup_count(a().entries[i].dirent.inode());
    if (generation.has_value()) {
      use_records.emplace_back(a().entries[i].dirent.inode(), *generation);
    }
  }

  if (use_records.empty()) {
    return std::move(collector).finish();
  }

  for (const auto &[record_ino, generation] : use_records) {
    const auto use_key = pack_inode_use_key(record_ino);
    const uint64_t generation_le = htole64(generation);
    fdb_transaction_atomic_op(transaction.get(), use_key.data(), use_key.size(),
                              reinterpret_cast<const uint8_t *>(&generation_le),
                              sizeof(generation_le), FDB_MUTATION_TYPE_MAX);
  }

  a().reply_collector.emplace(std::move(collector));
  return commit(std::bind(&Inflight_readdirplus<ActionT>::reply_buffer, this));
}

template <typename ActionT>
ActionT Inflight_readdirplus<ActionT>::reply_buffer() {
  if (!a().reply_collector.has_value()) {
    return ActionT::Abort(EIO);
  }
  return std::move(*(a().reply_collector)).finish();
}
