#ifndef __INFLIGHT_H_
#define __INFLIGHT_H_

#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <set>
#include <source_location>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>

#if DEBUG
#include <time.h>
#endif

#include "fsync.h"
#include "util.h"

// Yes: write op that needs oplog protection for maybe-committed retries.
// IdempotentWrite: write op that can be safely retried without oplog check.
// ReadOnly: no writes.
enum class ReadWrite { Yes, IdempotentWrite, ReadOnly };

// Fresh: fetch a new read version on the first try
// Cached: using the cached read version is
enum class ReadVersion { Fresh, Cached };

template <ReadWrite RW, ReadVersion RV = ReadVersion::Cached,
          bool UsesOplog = (RW == ReadWrite::Yes)>
struct InflightPolicy {
  static constexpr ReadWrite read_write = RW;
  static constexpr ReadVersion read_version = RV;
  static constexpr bool uses_oplog = UsesOplog;
};

using InflightPolicyWrite = InflightPolicy<ReadWrite::Yes>;
using InflightPolicyIdempotentWrite =
    InflightPolicy<ReadWrite::IdempotentWrite>;
using InflightPolicyReadOnly = InflightPolicy<ReadWrite::ReadOnly>;

struct InflightRuntimePolicy {
  ReadWrite read_write;
  ReadVersion read_version;
  bool uses_oplog;
};

using OpLogResultVariant =
    std::variant<OpLogResultOK, OpLogResultEntry, OpLogResultAttr,
                 OpLogResultOpen, OpLogResultBuf, OpLogResultReadlink,
                 OpLogResultWrite, OpLogResultStatfs, OpLogResultXattrSize>;

template <typename ActionT> using InflightCallbackT = std::function<ActionT()>;

enum class DirentAddError {
  NoSpace,
  InvalidInput,
};

using DirentAddResult = std::expected<void, DirentAddError>;

template <typename ActionT> struct AttemptStateT {
  std::optional<InflightCallbackT<ActionT>> cb;
  unique_future commit;
  unique_future oplog_fetch;
  std::queue<FDBFuture *> future_queue;
  virtual ~AttemptStateT() = default;
};

template <typename ActionT> class InflightT {
public:
  // issuer is what we'll have to run if the future fails.
  // it'll expect the transaction to have been reset
  // (which is guaranteed by fdb)
  [[nodiscard]] virtual InflightCallbackT<ActionT> issue() = 0;

  void future_ready(FDBFuture *);

  virtual ~InflightT() = default;

  // the transaction we're tied to. will we ever want
  // two separate chains of computation using the same
  // transaction?
  unique_transaction transaction;

  // always need this so that our error processing code
  // can throw errors back to fuse.
  fuse_req_t req;
  bool suppress_errors = false;

  void start();

  ActionT commit(InflightCallbackT<ActionT>);

  // run before delete, in case there is anything a subclass
  // wants to take care of.
  void cleanup();
  void set_on_done(std::function<void()> callback);

  inline ReadWrite read_write() { return policy->read_write; };

  AttemptStateT<ActionT> &attempt_state();
  const AttemptStateT<ActionT> &attempt_state() const;

protected:
  // constructor
  InflightT(fuse_req_t, const InflightRuntimePolicy &, unique_transaction);

  // Per-attempt transaction configuration hook. Called from start() before any
  // reads/writes (including oplog checks).
  [[nodiscard]] virtual fdb_error_t configure_transaction() { return 0; }

  void wait_on_future(FDBFuture *, unique_future &);
  [[nodiscard]] virtual std::unique_ptr<AttemptStateT<ActionT>>
  create_attempt_state() = 0;
  void track_inode_for_fsync(fdbfs_ino_t ino);
  [[nodiscard]] virtual ActionT oplog_recovery(const OpLogRecord &);
  void reset_attempt_state();
  [[nodiscard]] bool should_check_oplog() const;
  [[nodiscard]] bool uses_oplog() const;
  [[nodiscard]] bool write_oplog_record(OpLogRecord record);
  [[nodiscard]] bool write_oplog_result(const OpLogResultVariant &);

private:
  // static behavior selected by the subclass policy.
  const InflightRuntimePolicy *policy;
  // single callback for completion, intended for use by FilehandleSerializer
  std::function<void()> on_done;
  std::unordered_map<fdbfs_ino_t, FsyncBarrierTable::Token> fsync_tokens;
  bool commit_unknown_seen = false;
  std::optional<uint64_t> op_id;
  std::unique_ptr<AttemptStateT<ActionT>> attempt;
  [[nodiscard]] ActionT check_oplog_or_issue();
  bool run_current_callback();
  void begin_wait();
  bool retry_with_on_error(fdb_error_t err, const char *source);
  void fail(int err, const char *why,
            std::source_location loc = std::source_location::current());
  static void error_processor(FDBFuture *, void *);
  static void error_checker(FDBFuture *, void *);

#if DEBUG
  struct timespec clockstart;
#endif
};

template <typename AttemptT, typename Policy, typename ActionT>
class InflightWithAttemptT : public InflightT<ActionT> {
public:
  static constexpr InflightRuntimePolicy runtime_policy{
      .read_write = Policy::read_write,
      .read_version = Policy::read_version,
      .uses_oplog = Policy::uses_oplog,
  };

  InflightWithAttemptT(fuse_req_t req, unique_transaction transaction)
      : InflightT<ActionT>(req, runtime_policy, std::move(transaction)) {
    this->reset_attempt_state();
  }

protected:
  AttemptT &a() { return static_cast<AttemptT &>(this->attempt_state()); }
  const AttemptT &a() const {
    return static_cast<const AttemptT &>(this->attempt_state());
  }

private:
  std::unique_ptr<AttemptStateT<ActionT>> create_attempt_state() override {
    return std::make_unique<AttemptT>();
  }
};

template <typename ActionT>
struct AttemptState_markusedT : public AttemptStateT<ActionT> {
  unique_future inode_fetch;
};

template <typename ActionT>
class Inflight_markusedT
    : public InflightWithAttemptT<AttemptState_markusedT<ActionT>,
                                  InflightPolicyIdempotentWrite, ActionT> {
public:
  Inflight_markusedT(fuse_req_t, uint64_t, struct fuse_entry_param,
                     unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  ActionT check_inode();
  ActionT reply_entry();

  const uint64_t generation;
  const struct fuse_entry_param entry;
};

#include "inflight.tpp"

#endif
