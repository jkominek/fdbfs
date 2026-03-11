
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>

#include "fdbfs_ops.h"
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
  using Base::a;
  using Base::req;
  using Base::transaction;
  using Base::wait_on_future;

  Inflight_readdir(fuse_req_t, fuse_ino_t,
                   typename ActionT::DirentCollectorSpec, off_t,
                   unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  const fuse_ino_t ino;
  const typename ActionT::DirentCollectorSpec collector_spec;
  const off_t off;

  ActionT callback();
};

template <typename ActionT>
Inflight_readdir<ActionT>::Inflight_readdir(
    fuse_req_t req, fuse_ino_t ino,
    typename ActionT::DirentCollectorSpec collector_spec, off_t off,
    unique_transaction transaction)
    : Base(req, std::move(transaction)), ino(ino),
      collector_spec(std::move(collector_spec)), off(off) {}

template <typename ActionT> ActionT Inflight_readdir<ActionT>::callback() {
  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_error_t err;

  err = fdb_future_get_keyvalue_array(a().range_fetch.get(), &kvs, &kvcount,
                                      &more);
  if (err)
    return ActionT::FDBError(err);

  auto collector = ActionT::make_dirent_collector(req, off, collector_spec);

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
  const auto [start, stop] = pack_dentry_subspace_range(ino);

  auto collector = ActionT::make_dirent_collector(req, off, collector_spec);
  const size_t estimated_count = collector.estimate_remaining_entries();
  int limit = static_cast<int>(estimated_count);
  limit = std::max<size_t>(8, limit);

  // well this is tricky. how large a range should we request?
  wait_on_future(
      fdb_transaction_get_range(transaction.get(), start.data(), start.size(),
                                0, 1 + off, stop.data(), stop.size(), 0, 1,
                                limit, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
      a().range_fetch);
  return std::bind(&Inflight_readdir<ActionT>::callback, this);
}

extern "C" void fdbfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                              off_t off, struct fuse_file_info *fi) {
  // fuse will know we're out of entries because we'll return 0
  // entries to a call here. but there's a decent chance we'll
  // find out on the previous call that there wasn't anything left.
  // could we fast path that case?
  // perhaps fdbfs_readdir_callback when it sees it has reached
  // the end, could set the final offset to all-1s, and we could
  // detect that here and immediately return an empty result?
  // or we could maintain a cache of how many entries we last
  // saw in a given directory. that'd let us do a better job of
  // fetching them, and if the cache entry is recent enough, and
  // we're being asked to read past the end, we could maybe bail
  // early, here.

  // let's not read much more than 64k in a go.
  auto collector_spec = FuseInflightAction::make_dirent_collector_spec(
      std::min(size, static_cast<size_t>(1 << 16)));
  auto *inflight = new Inflight_readdir<FuseInflightAction>(
      req, ino, collector_spec, off, make_transaction());

  inflight->start();
}
