#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <cstdint>
#include <utility>
#include <vector>

#include "inflight.h"
#include "util.h"

struct AtomicFieldOp {
  std::vector<uint8_t> suffix;
  FDBMutationType mutation_type;
  std::vector<uint8_t> value;
};

template <typename ActionT>
struct AttemptState_atomicfield : public AttemptStateT<ActionT> {};

template <typename ActionT>
class Inflight_atomicfield
    : public InflightWithAttemptT<AttemptState_atomicfield<ActionT>,
                                  InflightPolicyIdempotentWrite, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_atomicfield<ActionT>,
                                    InflightPolicyIdempotentWrite, ActionT>;
  using req_t = typename ActionT::req_t;
  using Base::a;
  using Base::track_inode_for_fsync;
  using Base::transaction;
  using Base::wait_on_future;

  Inflight_atomicfield(req_t, fdbfs_ino_t, std::vector<AtomicFieldOp>,
                       unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  const fdbfs_ino_t ino;
  const std::vector<AtomicFieldOp> ops;
};

template <typename ActionT>
Inflight_atomicfield<ActionT>::Inflight_atomicfield(
    req_t req, fdbfs_ino_t ino, std::vector<AtomicFieldOp> ops,
    unique_transaction transaction)
    : Base(req, std::move(transaction)), ino(ino), ops(std::move(ops)) {
  track_inode_for_fsync(ino);
}

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_atomicfield<ActionT>::issue() {
  for (const auto &op : ops) {
    const auto key = pack_inode_field_key(ino, op.suffix);
    fdb_transaction_atomic_op(transaction.get(), key.data(),
                              static_cast<int>(key.size()), op.value.data(),
                              static_cast<int>(op.value.size()),
                              op.mutation_type);
  }

  wait_on_future(fdb_transaction_commit(transaction.get()), a().commit);
  return ActionT::OK;
}
