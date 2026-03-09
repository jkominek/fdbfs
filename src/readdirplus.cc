#include <errno.h>

#include <algorithm>
#include <string>
#include <vector>

#include "fdbfs_ops.h"
#include "inflight.h"
#include "util.h"
#include "values.pb.h"

/*************************************************************
 * readdirplus
 *************************************************************
 */

struct ReaddirPlusEntry {
  std::string name;
  fuse_ino_t ino;
};

template <typename ActionT>
struct AttemptState_readdirplus : public AttemptStateT<ActionT> {
  unique_future range_fetch;
  std::vector<ReaddirPlusEntry> entries;
  std::vector<unique_future> inode_fetches;
  std::vector<uint8_t> reply_buf;
  int reply_size = 0;
};

template <typename ActionT>
class Inflight_readdirplus
    : public InflightWithAttemptT<AttemptState_readdirplus<ActionT>,
                                  InflightPolicyReadOnly, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_readdirplus<ActionT>,
                                    InflightPolicyReadOnly, ActionT>;
  using Base::a;
  using Base::commit;
  using Base::req;
  using Base::transaction;
  using Base::wait_on_future;

  Inflight_readdirplus(fuse_req_t, fuse_ino_t, size_t, off_t,
                       unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  ActionT dirent_callback();
  ActionT callback();
  ActionT reply_buffer();

  const fuse_ino_t ino;
  const size_t size;
  const off_t off;
};

template <typename ActionT>
Inflight_readdirplus<ActionT>::Inflight_readdirplus(
    fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
    unique_transaction transaction)
    : Base(req, std::move(transaction)), ino(ino), size(size),
      off(off) {}

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_readdirplus<ActionT>::issue() {
  const auto [start, stop] = pack_dentry_subspace_range(ino);

  struct fuse_entry_param dummy{};
  size_t estimated_entry_size =
      fuse_add_direntry_plus(req, nullptr, 0, "12345678", &dummy, 1);
  if (estimated_entry_size == 0) {
    estimated_entry_size = 1;
  }
  const size_t estimated_count =
      std::max<size_t>(1, size / estimated_entry_size);
  // limit ourselves to pulling 128 entries at once, since that's a decent
  // amount of potential traffic.
  const int limit = static_cast<int>(std::min<size_t>(128, estimated_count));
  const int offset = static_cast<int>(off);

  wait_on_future(
      fdb_transaction_get_range(transaction.get(), start.data(), start.size(),
                                0, 1 + offset, stop.data(), stop.size(), 0, 1,
                                limit, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
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
        .ino = dirent.inode(),
    };
    a().entries.emplace_back(std::move(entry));

    const auto inode_key = pack_inode_key(dirent.inode());
    a().inode_fetches.emplace_back();
    wait_on_future(fdb_transaction_get(transaction.get(), inode_key.data(),
                                       inode_key.size(), 1),
                   a().inode_fetches.back());
  }

  if (a().inode_fetches.empty()) {
    return ActionT::Buf({});
  }
  return ActionT::BeginWait(
      std::bind(&Inflight_readdirplus<ActionT>::callback, this));
}

template <typename ActionT>
ActionT Inflight_readdirplus<ActionT>::callback() {
  if (a().inode_fetches.size() != a().entries.size()) {
    return ActionT::Abort(EIO);
  }

  std::vector<uint8_t> buf(size);
  size_t consumed_buffer = 0;
  size_t remaining_buffer = size;

  std::vector<std::pair<fuse_ino_t, uint64_t>> use_records;
  use_records.reserve(a().entries.size());

  for (size_t i = 0; i < a().entries.size(); i++) {
    fdb_bool_t present = 0;
    const uint8_t *val = nullptr;
    int vallen = 0;
    const fdb_error_t err = fdb_future_get_value(a().inode_fetches[i].get(),
                                                 &present, &val, &vallen);
    if (err) {
      return ActionT::FDBError(err);
    }
    if (!present) {
      // this is some sort of db corruption; can we maybe stuff an
      // error into the response, or fill it out partially, so that
      // users can at least see which dirent is misbehaving?
      return ActionT::Abort(EIO);
    }

    INodeRecord inode;
    if (!inode.ParseFromArray(val, vallen) || !inode.IsInitialized()) {
      return ActionT::Abort(EIO);
    }
    if (inode.inode() != a().entries[i].ino) {
      return ActionT::Abort(EIO);
    }

    struct fuse_entry_param e{};
    e.ino = a().entries[i].ino;
    e.generation = 1;
    pack_inode_record_into_stat(inode, e.attr);
    e.attr_timeout = 0.01;
    e.entry_timeout = 0.01;

    const size_t used = fuse_add_direntry_plus(
        req, reinterpret_cast<char *>(buf.data() + consumed_buffer),
        remaining_buffer, a().entries[i].name.c_str(), &e,
        off + static_cast<off_t>(i) + 1);
    if (used > remaining_buffer) {
      break;
    }

    consumed_buffer += used;
    remaining_buffer -= used;

    auto generation = increment_lookup_count(e.ino);
    if (generation.has_value()) {
      use_records.emplace_back(e.ino, *generation);
    }
  }

  if (use_records.empty()) {
    return ActionT::Buf(std::move(buf), static_cast<int>(consumed_buffer));
  }

  for (const auto &[record_ino, generation] : use_records) {
    const auto use_key = pack_inode_use_key(record_ino);
    const uint64_t generation_le = htole64(generation);
    fdb_transaction_atomic_op(transaction.get(), use_key.data(), use_key.size(),
                              reinterpret_cast<const uint8_t *>(&generation_le),
                              sizeof(generation_le), FDB_MUTATION_TYPE_MAX);
  }

  a().reply_buf = std::move(buf);
  a().reply_size = static_cast<int>(consumed_buffer);
  return commit(std::bind(&Inflight_readdirplus<ActionT>::reply_buffer, this));
}

template <typename ActionT>
ActionT Inflight_readdirplus<ActionT>::reply_buffer() {
  return ActionT::Buf(std::move(a().reply_buf), a().reply_size);
}

extern "C" void fdbfs_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size,
                                  off_t off, struct fuse_file_info *fi) {
  (void)fi;
  auto *inflight = new Inflight_readdirplus<FuseInflightAction>(
      req, ino, size, off, make_transaction());
  inflight->start();
}
