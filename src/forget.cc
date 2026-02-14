
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fdbfs_ops.h"
#include "inflight.h"
#include "util.h"

/*************************************************************
 * forget
 *************************************************************
 */
struct AttemptState_forget : public AttemptState {};
struct ForgetEntry {
  fuse_ino_t ino;
  uint64_t generation;
};

class Inflight_forget : public InflightWithAttempt<AttemptState_forget> {
public:
  Inflight_forget(fuse_req_t, std::vector<ForgetEntry>, unique_transaction);
  InflightCallback issue();

private:
  const std::vector<ForgetEntry> entries;
};

Inflight_forget::Inflight_forget(fuse_req_t req,
                                 std::vector<ForgetEntry> entries,
                                 unique_transaction transaction)
    : InflightWithAttempt(req, ReadWrite::Yes, std::move(transaction)),
      entries(std::move(entries)) {}

InflightCallback Inflight_forget::issue() {
  for (const auto &entry : entries) {
    const auto key = pack_inode_use_key(entry.ino);
    const uint64_t generation_le = htole64(entry.generation);
    // NOTE this is still a sort of optimized version. really we want
    // to remove any use record OLDER than the generation we've been told
    // to remove. but that requires a round trip. so we'll leave this
    // as the compare and clear for now, on the theory that we might just
    // develop a better way of dealing with this in the future.
    fdb_transaction_atomic_op(transaction.get(), key.data(), key.size(),
                              reinterpret_cast<const uint8_t *>(&generation_le),
                              sizeof(generation_le),
                              FDB_MUTATION_TYPE_COMPARE_AND_CLEAR);
  }

  wait_on_future(fdb_transaction_commit(transaction.get()), a().commit);
  return InflightAction::None;
}

extern "C" void fdbfs_forget(fuse_req_t req, fuse_ino_t ino, uint64_t ncount) {
  // we've only got to issue an fdb transaction if decrement says so
  auto generation = decrement_lookup_count(ino, ncount);
  if (generation.has_value()) {
    std::vector<ForgetEntry> entries(1);
    entries[0] = ForgetEntry{ino, *generation};
    Inflight_forget *inflight =
        new Inflight_forget(req, std::move(entries), make_transaction());
    inflight->start();
  } else {
    fuse_reply_none(req);
  }
}

extern "C" void fdbfs_forget_multi(fuse_req_t req, size_t count,
                                   struct fuse_forget_data *forgets) {
  std::vector<ForgetEntry> entries;
  entries.reserve(count);
  for (size_t i = 0; i < count; i++) {
    auto generation =
        decrement_lookup_count(forgets[i].ino, forgets[i].nlookup);
    if (generation.has_value()) {
      entries.push_back(ForgetEntry{forgets[i].ino, *generation});
    }
  }
  if (entries.size() > 0) {
    // we've got to issue forgets
    Inflight_forget *inflight =
        new Inflight_forget(req, std::move(entries), make_transaction());
    inflight->start();
  } else {
    fuse_reply_none(req);
  }
}
