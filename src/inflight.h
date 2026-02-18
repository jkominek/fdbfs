#ifndef __INFLIGHT_H_
#define __INFLIGHT_H_

#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

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
#include <utility>
#include <variant>

#if DEBUG
#include <time.h>
#endif

#include "util.h"

#include "thread_pool.hpp"

// Pool used for processing our callbacks
extern thread_pool pool;

// Halt all inflights and prevent new ones from starting.
extern void shut_it_down();

// Yes: write op that needs oplog protection for maybe-committed retries.
// IdempotentWrite: write op that can be safely retried without oplog check.
// ReadOnly: no writes.
enum class ReadWrite { Yes, IdempotentWrite, ReadOnly };

using OpLogResultVariant =
    std::variant<OpLogResultOK, OpLogResultEntry, OpLogResultAttr,
                 OpLogResultOpen, OpLogResultBuf, OpLogResultReadlink,
                 OpLogResultWrite, OpLogResultStatfs, OpLogResultXattrSize>;

// op log helpers
[[nodiscard]] extern std::vector<uint8_t>
pack_oplog_key(const std::vector<uint8_t> &owner_pid, uint64_t op_id);
[[nodiscard]] extern range_keys
pack_oplog_subspace_range(const std::vector<uint8_t> &owner_pid);
[[nodiscard]] extern range_keys
pack_local_oplog_span_range(uint64_t start_op_id, uint64_t stop_op_id);
[[nodiscard]] extern std::optional<std::pair<uint64_t, uint64_t>>
claim_local_oplog_cleanup_span();

class InflightAction;

typedef std::function<InflightAction()> InflightCallback;

extern "C" void fdbfs_error_processor(FDBFuture *, void *);
extern "C" void fdbfs_error_checker(FDBFuture *, void *);

struct AttemptState {
  std::optional<InflightCallback> cb;
  unique_future commit;
  unique_future oplog_fetch;
  std::queue<FDBFuture *> future_queue;
  virtual ~AttemptState() = default;
};

class Inflight {
public:
  // issuer is what we'll have to run if the future fails.
  // it'll expect the transaction to have been reset
  // (which is guaranteed by fdb)
  [[nodiscard]] virtual InflightCallback issue() = 0;

  void future_ready(FDBFuture *);

  virtual ~Inflight() = default;

  // the transaction we're tied to. will we ever want
  // two separate chains of computation using the same
  // transaction?
  unique_transaction transaction;

  // always need this so that our error processing code
  // can throw errors back to fuse.
  fuse_req_t req;
  bool suppress_errors = false;

  void start();

  InflightAction commit(InflightCallback);

  // run before delete, in case there is anything a subclass
  // wants to take care of.
  void cleanup();

protected:
  // constructor
  Inflight(fuse_req_t, ReadWrite, unique_transaction);

  // Per-attempt transaction configuration hook. Called from start() before any
  // reads/writes (including oplog checks).
  [[nodiscard]] virtual fdb_error_t configure_transaction() { return 0; }

  void wait_on_future(FDBFuture *, unique_future &);
  [[nodiscard]] virtual std::unique_ptr<AttemptState>
  create_attempt_state() = 0;
  [[nodiscard]] virtual InflightAction oplog_recovery(const OpLogRecord &);
  AttemptState &attempt_state();
  const AttemptState &attempt_state() const;
  void reset_attempt_state();
  [[nodiscard]] bool should_check_oplog() const;
  [[nodiscard]] bool uses_oplog() const;
  [[nodiscard]] bool write_oplog_record(OpLogRecord record);
  [[nodiscard]] bool write_oplog_result(const OpLogResultVariant &);

private:
  // whether we're intended as r/w or not.
  ReadWrite readwrite;
  bool commit_unknown_seen = false;
  std::optional<uint64_t> op_id;
  std::unique_ptr<AttemptState> attempt;
  [[nodiscard]] InflightAction check_oplog_or_issue();
  bool run_current_callback();
  void begin_wait();
  bool retry_with_on_error(fdb_error_t err, const char *source);

#if DEBUG
  struct timespec clockstart;
#endif

  friend class InflightAction;
  friend void fdbfs_error_processor(FDBFuture *, void *);
  friend void fdbfs_error_checker(FDBFuture *, void *);
};

template <typename AttemptT> class InflightWithAttempt : public Inflight {
public:
  InflightWithAttempt(fuse_req_t req, ReadWrite readwrite,
                      unique_transaction transaction)
      : Inflight(req, readwrite, std::move(transaction)) {
    reset_attempt_state();
  }

protected:
  AttemptT &a() { return static_cast<AttemptT &>(attempt_state()); }
  const AttemptT &a() const {
    return static_cast<const AttemptT &>(attempt_state());
  }

private:
  std::unique_ptr<AttemptState> create_attempt_state() override {
    return std::make_unique<AttemptT>();
  }
};

struct AttemptState_markused : public AttemptState {
  unique_future inode_fetch;
};

class Inflight_markused : public InflightWithAttempt<AttemptState_markused> {
public:
  Inflight_markused(fuse_req_t, uint64_t, struct fuse_entry_param,
                    unique_transaction);
  InflightCallback issue();

private:
  InflightAction check_inode();
  InflightAction reply_entry();

  const uint64_t generation;
  const struct fuse_entry_param entry;
};

// TODO consider how we might pull the FUSE bits out of these,
// or otherwise abstract what function is being called when one
// of these is 'executed', so that we can swap in a different
// back end for testing, or other fancy things
// Make InflightAction a virtual base class, and have, for instance,
// a FUSEInflightAction which is passed in to the function, and it
// can call methods off of that.
class InflightAction {
public:
  static bool trace_errors_enabled() {
    static const bool enabled = (getenv("FDBFS_TRACE_ERRORS") != nullptr);
    return enabled;
  }
  static void trace_errno_error(const char *kind, int err, const char *why,
                                const std::source_location &loc) {
    if (!trace_errors_enabled())
      return;
    fprintf(stderr, "fdbfs %s: err=%d (%s) at %s:%u (%s)%s%s\n", kind, err,
            strerror(err), loc.file_name(), loc.line(), loc.function_name(),
            (why && why[0]) ? " why=" : "", (why && why[0]) ? why : "");
  }
  static void trace_fdb_error(const char *kind, fdb_error_t err,
                              const char *why,
                              const std::source_location &loc) {
    if (!trace_errors_enabled())
      return;
    fprintf(stderr, "fdbfs %s: fdb_err=%d (%s) at %s:%u (%s)%s%s\n", kind, err,
            fdb_get_error(err), loc.file_name(), loc.line(),
            loc.function_name(), (why && why[0]) ? " why=" : "",
            (why && why[0]) ? why : "");
  }
  static InflightAction BeginWait(InflightCallback newcb) {
    return InflightAction(false, true, false, [newcb](Inflight *i) {
      i->attempt_state().cb.emplace(newcb);
    });
  }
  // Error surfaced from fdb_transaction_* API calls where FoundationDB expects
  // us to run fdb_transaction_on_error for retryable codes.
  static InflightAction FDBTransactionError(
      fdb_error_t err, const char *why = "",
      std::source_location loc = std::source_location::current()) {
    trace_fdb_error("FDBTransactionError", err, why, loc);
    if (fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, err)) {
      // retryable FDB errors must flow through fdb_transaction_on_error.
      return InflightAction(false, false, false, [](Inflight *) {}, err);
    } else {
      // can't be retried, surface an error.
      return InflightAction(true, false, false, [](Inflight *i) {
        // NOTE we could, perhaps, improve this slightly by digging out
        // a list of fdb_error_t values from the code and doing a switch
        // on them, but that's not stable, and wouldn't add much value.
        fuse_reply_err(i->req, EIO);
      });
    }
  }

  // Error surfaced from fdb_future_get_* accessors (or other unexpected API
  // surfaces). By the time we are decoding a ready future, transaction errors
  // should already have been routed via fdb_future_get_error in the checker.
  // Treat these as internal failures, not on_error retry points.
  static InflightAction
  FDBError(fdb_error_t err, const char *why = "",
           std::source_location loc = std::source_location::current()) {
    trace_fdb_error("FDBError", err, why, loc);
    return InflightAction::Abort(EIO, why, loc);
  }
  static InflightAction Restart() {
    // TODO someday track how many restarts we've done, so that after
    // N of them, we can switch to an abort?
    return InflightAction(false, false, true, [](Inflight *) {});
  }
  static InflightAction None() {
    return InflightAction(true, false, false,
                          [](Inflight *i) { fuse_reply_none(i->req); });
  };
  static InflightAction Ignore() {
    return InflightAction(true, false, false, [](Inflight *i) {

    });
  };
  static InflightAction OK() {
    return InflightAction(true, false, false,
                          [](Inflight *i) { fuse_reply_err(i->req, 0); });
  };
  static InflightAction
  Abort(int err, const char *why = "",
        std::source_location loc = std::source_location::current()) {
    trace_errno_error("Abort", err, why, loc);
    return InflightAction(true, false, false,
                          [err](Inflight *i) { fuse_reply_err(i->req, err); });
  }
  static InflightAction Entry(struct fuse_entry_param e) {
    return InflightAction(true, false, false, [e](Inflight *i) {
      auto generation = increment_lookup_count(e.ino);
      if (generation.has_value()) {
        // first lookup for this inode in local kernel cache; publish
        // a use record before replying to fuse.
        (new Inflight_markused(i->req, *generation, e, make_transaction()))
            ->start();
      } else {
        // already known locally; no new use record needed.
        fuse_reply_entry(i->req, &e);
      }
    });
  }
  static InflightAction Attr(struct stat attr) {
    return InflightAction(true, false, false, [attr](Inflight *i) {
      fuse_reply_attr(i->req, &attr, 0.0);
    });
  }
  static InflightAction Open(fuse_ino_t ino, struct fuse_file_info fi) {
    return InflightAction(true, false, false, [ino, fi](Inflight *i) mutable {
      (void)reply_open_with_handle(i->req, ino, &fi);
    });
  }
  static InflightAction Buf(std::vector<uint8_t> buf, int actual_size = -1) {
    // Note, per the default value for actual_size, we might receive
    // a buffer which is larger than the amount of useful/valid data
    // in it. By passing in an actual_size value, calling code can
    // restrict the amount of buf which is passed along.
    return InflightAction(true, false, false, [buf, actual_size](Inflight *i) {
      fuse_reply_buf(i->req, reinterpret_cast<const char *>(buf.data()),
                     (actual_size >= 0) ? actual_size : buf.size());
    });
  }
  static InflightAction Readlink(std::string name) {
    return InflightAction(true, false, false, [name](Inflight *i) {
      fuse_reply_readlink(i->req, name.c_str());
    });
  }
  static InflightAction Write(size_t size) {
    return InflightAction(true, false, false, [size](Inflight *i) {
      fuse_reply_write(i->req, size);
    });
  }
  static InflightAction Statfs(std::shared_ptr<struct statvfs> statbuf) {
    return InflightAction(true, false, false, [statbuf](Inflight *i) {
      fuse_reply_statfs(i->req, statbuf.get());
    });
  }
  static InflightAction XattrSize(ssize_t size) {
    return InflightAction(true, false, false, [size](Inflight *i) {
      fuse_reply_xattr(i->req, size);
    });
  }

protected:
  InflightAction(bool delete_this, bool begin_wait, bool restart,
                 std::function<void(Inflight *)> perform,
                 std::optional<fdb_error_t> retryable_err = std::nullopt)
      : delete_this(delete_this), begin_wait(begin_wait), restart(restart),
        perform(std::move(perform)), retryable_err(retryable_err) {};

  bool delete_this = false;
  bool begin_wait = false;
  bool restart = false;
  std::function<void(Inflight *)> perform;
  std::optional<fdb_error_t> retryable_err;

  friend class Inflight;
};

#endif // __INFLIGHT_H_
