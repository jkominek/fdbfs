
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

struct AttemptState_readdir : public AttemptState {
  unique_future range_fetch;
};

class Inflight_readdir
    : public InflightWithAttempt<AttemptState_readdir, InflightPolicyReadOnly> {
public:
  Inflight_readdir(fuse_req_t, fuse_ino_t, size_t, off_t, unique_transaction);
  InflightCallback issue();

private:
  const fuse_ino_t ino;
  const size_t size;
  const off_t off;

  InflightAction callback();
};

Inflight_readdir::Inflight_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                                   off_t off, unique_transaction transaction)
    : InflightWithAttempt(req, std::move(transaction)), ino(ino), size(size),
      off(off) {}

InflightAction Inflight_readdir::callback() {
  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_error_t err;

  err = fdb_future_get_keyvalue_array(a().range_fetch.get(), &kvs, &kvcount,
                                      &more);
  if (err)
    return InflightAction::FDBError(err);

  std::vector<uint8_t> buf(size);
  size_t consumed_buffer = 0;
  size_t remaining_buffer = size;

  for (int i = 0; i < kvcount; i++) {
    FDBKeyValue kv = kvs[i];

    if (kv.key_length <= dirent_prefix_length) {
      // serious internal error. we somehow got back a key that was too short?
      return InflightAction::Abort(EIO);
    }
    int keylen = kv.key_length - dirent_prefix_length;
    if ((keylen <= 0) || (keylen > MAXFILENAMELEN)) {
      // internal error
      return InflightAction::Abort(EIO);
    }
    char name[MAXFILENAMELEN + 1];
    bcopy(((uint8_t *)kv.key) + dirent_prefix_length, name, keylen);
    name[keylen] = '\0'; // null terminate

    struct stat attr;
    {
      DirectoryEntry dirent;
      dirent.ParseFromArray(kv.value, kv.value_length);

      if (!dirent.IsInitialized()) {
        return InflightAction::Abort(EIO);
      }
      attr.st_ino = dirent.inode();
      attr.st_mode = dirent.type();
    }

    size_t used = fuse_add_direntry(
        req, reinterpret_cast<char *>(buf.data() + consumed_buffer),
        remaining_buffer, name, &attr, off + i + 1);
    if (used > remaining_buffer) {
      // ran out of space. last one failed. we're done.
      break;
    }

    consumed_buffer += used;
    remaining_buffer -= used;
  }

  buf.resize(consumed_buffer);
  return InflightAction::Buf(buf);
}

InflightCallback Inflight_readdir::issue() {
  const auto [start, stop] = pack_dentry_subspace_range(ino);

  // Estimate how many entries can fit in the caller-provided buffer.
  // We use an 8-byte representative filename to avoid wildly
  // overestimating the count for typical names.
  struct stat dummy_attr{};
  size_t estimated_entry_size =
      fuse_add_direntry(req, nullptr, 0, "12345678", &dummy_attr, off + 1);
  if (estimated_entry_size == 0) {
    estimated_entry_size = 1;
  }
  const size_t estimated_count =
      std::max<size_t>(1, size / estimated_entry_size);
  const int limit = static_cast<int>(estimated_count);

  // well this is tricky. how large a range should we request?
  wait_on_future(
      fdb_transaction_get_range(transaction.get(), start.data(), start.size(),
                                0, 1 + off, stop.data(), stop.size(), 0, 1,
                                limit, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
      a().range_fetch);
  return std::bind(&Inflight_readdir::callback, this);
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
  Inflight_readdir *inflight = new Inflight_readdir(
      req, ino, std::min(size, static_cast<size_t>(1 << 16)), off,
      make_transaction());

  inflight->start();
}
