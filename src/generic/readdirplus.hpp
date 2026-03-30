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
  unique_future dirinode_fetch;
  unique_future parent_inode_fetch;
  unique_future range_fetch;
  std::vector<ReaddirPlusEntry> entries;
  std::vector<unique_future> inode_fetches;
  std::optional<INodeRecord> dirinode;
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
                       typename ActionT::DirentCollectorSpec, ReaddirStartKind,
                       std::string_view, unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  ActionT dirent_callback();
  ActionT callback();
  ActionT reply_buffer();

  const fdbfs_ino_t ino;
  const typename ActionT::DirentCollectorSpec collector_spec;
  const ReaddirStartKind start_kind;
  const std::string start_name;
};

template <typename ActionT>
Inflight_readdirplus<ActionT>::Inflight_readdirplus(
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

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_readdirplus<ActionT>::issue() {
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
  bool fetch_dirinode = false;
  if (start_kind == ReaddirStartKind::Beginning) {
    estimated_count -= 2;
    fetch_dirinode = true;
  } else if (start_kind == ReaddirStartKind::AfterDot) {
    estimated_count -= 1;
    fetch_dirinode = true;
  }
  if (fetch_dirinode) {
    const auto [dirinode_start, dirinode_stop] =
        pack_inode_and_fields_range(ino);
    wait_on_future(fdb_transaction_get_range(
                       transaction.get(),
                       FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(dirinode_start.data(),
                                                         dirinode_start.size()),
                       FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(dirinode_stop.data(),
                                                         dirinode_stop.size()),
                       4, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
                   a().dirinode_fetch);
  }
  // limit ourselves to pulling 128 entries at once, since that's a decent
  // amount of potential traffic.
  const int limit = std::max(2, std::min(128, estimated_count));

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

  a().entries.reserve(kvcount);
  a().inode_fetches.reserve(kvcount);

  if (a().dirinode_fetch) {
    const FDBKeyValue *inode_kvs = nullptr;
    int inode_kvcount = 0;
    fdb_bool_t inode_more = 0;
    const fdb_error_t inode_err = fdb_future_get_keyvalue_array(
        a().dirinode_fetch.get(), &inode_kvs, &inode_kvcount, &inode_more);
    if (inode_err) {
      return ActionT::FDBError(inode_err);
    }
    if (inode_kvcount < 1) {
      return ActionT::Abort(ENOENT);
    }

    const auto expected_key = pack_inode_key(ino);
    if ((inode_kvs[0].key_length != static_cast<int>(expected_key.size())) ||
        (std::memcmp(inode_kvs[0].key, expected_key.data(),
                     expected_key.size()) != 0)) {
      return ActionT::Abort(EIO);
    }

    INodeRecord dirinode;
    if (!dirinode.ParseFromArray(inode_kvs[0].value,
                                 inode_kvs[0].value_length) ||
        !dirinode.IsInitialized()) {
      return ActionT::Abort(EIO);
    }
    if (dirinode.inode() != ino) {
      return ActionT::Abort(EIO);
    }
    if (auto it = apply_newer_inode_time_fields(inode_kvs + 1,
                                                inode_kvcount - 1, dirinode);
        !it.has_value()) {
      return ActionT::Abort(it.error());
    }
    if ((dirinode.type() != ft_directory) || !dirinode.has_parentinode()) {
      return ActionT::Abort(EIO);
    }
    a().dirinode.emplace(std::move(dirinode));

    const auto [parent_start, parent_stop] =
        pack_inode_and_fields_range(a().dirinode->parentinode());
    wait_on_future(fdb_transaction_get_range(
                       transaction.get(),
                       FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(parent_start.data(),
                                                         parent_start.size()),
                       FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(parent_stop.data(),
                                                         parent_stop.size()),
                       4, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
                   a().parent_inode_fetch);
  }

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

  if (a().inode_fetches.empty() && !a().parent_inode_fetch) {
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
  auto add_synthetic =
      [&](std::string_view name, fdbfs_ino_t entry_ino,
          const INodeRecord &inode) -> std::expected<void, int> {
    DirectoryEntry dirent;
    dirent.set_inode(entry_ino);
    dirent.set_type(ft_directory);
    auto add_result = collector.try_add(name, dirent, &inode);
    if (!add_result.has_value()) {
      if (add_result.error() == DirentAddError::InvalidInput) {
        return std::unexpected(EIO);
      }
      return std::unexpected(0);
    }
    return {};
  };

  std::vector<std::pair<fdbfs_ino_t, uint64_t>> use_records;
  use_records.reserve(a().entries.size() + 2);

  std::optional<INodeRecord> parent_inode;
  if (a().parent_inode_fetch) {
    const FDBKeyValue *kvs = nullptr;
    int kvcount = 0;
    fdb_bool_t more = 0;
    const fdb_error_t err = fdb_future_get_keyvalue_array(
        a().parent_inode_fetch.get(), &kvs, &kvcount, &more);
    if (err) {
      return ActionT::FDBError(err);
    }
    if (kvcount < 1) {
      return ActionT::Abort(ENOENT);
    }

    assert(a().dirinode.has_value());
    const auto expected_key = pack_inode_key(a().dirinode->parentinode());
    if ((kvs[0].key_length != static_cast<int>(expected_key.size())) ||
        (std::memcmp(kvs[0].key, expected_key.data(), expected_key.size()) !=
         0)) {
      return ActionT::Abort(EIO);
    }

    parent_inode.emplace();
    if (!parent_inode->ParseFromArray(kvs[0].value, kvs[0].value_length) ||
        !parent_inode->IsInitialized()) {
      return ActionT::Abort(EIO);
    }
    if (parent_inode->inode() != a().dirinode->parentinode()) {
      return ActionT::Abort(EIO);
    }
    if (auto it =
            apply_newer_inode_time_fields(kvs + 1, kvcount - 1, *parent_inode);
        !it.has_value()) {
      return ActionT::Abort(it.error());
    }
  }

  switch (start_kind) {
  case ReaddirStartKind::Beginning:
    assert(a().dirinode.has_value());
    if (!add_synthetic(".", ino, *a().dirinode).has_value()) {
      return std::move(collector).finish();
    }
    if (auto generation = increment_lookup_count(ino); generation.has_value()) {
      use_records.emplace_back(ino, *generation);
    }
    [[fallthrough]];
  case ReaddirStartKind::AfterDot:
    assert(parent_inode.has_value());
    if (!add_synthetic("..", parent_inode->inode(), *parent_inode)
             .has_value()) {
      return std::move(collector).finish();
    }
    if (auto generation = increment_lookup_count(parent_inode->inode());
        generation.has_value()) {
      use_records.emplace_back(parent_inode->inode(), *generation);
    }
    break;
  case ReaddirStartKind::AfterDotDot:
  case ReaddirStartKind::AfterName:
    break;
  }

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
