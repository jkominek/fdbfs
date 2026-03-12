
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "inflight.h"
#include "util.h"

/*************************************************************
 * forget
 *************************************************************
 */
template <typename ActionT>
struct AttemptState_forget : public AttemptStateT<ActionT> {};
struct ForgetEntry {
  fdbfs_ino_t ino;
  uint64_t generation;
};

template <typename ActionT>
class Inflight_forget
    : public InflightWithAttemptT<AttemptState_forget<ActionT>,
                                  InflightPolicyIdempotentWrite, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_forget<ActionT>,
                                    InflightPolicyIdempotentWrite, ActionT>;
  using Base::a;
  using Base::transaction;
  using Base::wait_on_future;

  Inflight_forget(fuse_req_t, std::vector<ForgetEntry>, unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  const std::vector<ForgetEntry> entries;
};

template <typename ActionT>
Inflight_forget<ActionT>::Inflight_forget(fuse_req_t req,
                                          std::vector<ForgetEntry> entries,
                                          unique_transaction transaction)
    : Base(req, std::move(transaction)), entries(std::move(entries)) {}

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_forget<ActionT>::issue() {
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
  return ActionT::None;
}
