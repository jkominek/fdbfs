#ifndef __INFLIGHT_H_
#define __INFLIGHT_H_

#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>
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

#include "thread_pool.hpp"

// Pool used for processing our callbacks
extern thread_pool pool;

// Halt all inflights and prevent new ones from starting.
extern void shut_it_down();

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

[[nodiscard]] extern std::optional<std::pair<uint64_t, uint64_t>>
claim_local_oplog_cleanup_span();

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
  void track_inode_for_fsync(fuse_ino_t ino);
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
  std::unordered_map<fuse_ino_t, FsyncBarrierTable::Token> fsync_tokens;
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

class FuseInflightAction {
public:
  using Self = FuseInflightAction;
  struct DirentCollectorSpec {
    size_t max_bytes;
    bool plus_mode;
  };

  class DirentCollector {
  public:
    DirentCollector(fuse_req_t req, off_t start_off,
                    const DirentCollectorSpec &spec)
        : req(req), plus_mode(spec.plus_mode), next_offset(start_off + 1),
          buf(spec.max_bytes) {}

    [[nodiscard]] size_t estimate_remaining_entries() const {
      const size_t remaining = buf.size() - consumed;
      if (remaining == 0) {
        return 0;
      }

      if (plus_mode) {
        struct fuse_entry_param dummy{};
        size_t estimated_entry_size =
            fuse_add_direntry_plus(req, nullptr, 0, "12345678", &dummy, 1);
        if (estimated_entry_size == 0) {
          estimated_entry_size = 1;
        }
        return std::max<size_t>(1, remaining / estimated_entry_size);
      }

      struct stat dummy_attr{};
      size_t estimated_entry_size =
          fuse_add_direntry(req, nullptr, 0, "12345678", &dummy_attr, 1);
      if (estimated_entry_size == 0) {
        estimated_entry_size = 1;
      }
      return std::max<size_t>(1, remaining / estimated_entry_size);
    }

    [[nodiscard]] DirentAddResult
    try_add(std::string_view name, const DirectoryEntry &entry,
            const INodeRecord *inode = nullptr) {
      const size_t remaining = buf.size() - consumed;
      if (remaining == 0) {
        return std::unexpected(DirentAddError::NoSpace);
      }

      const std::string name_copy(name);

      if (plus_mode) {
        if (inode == nullptr) {
          return std::unexpected(DirentAddError::InvalidInput);
        }
        struct fuse_entry_param e{};
        e.ino = entry.inode();
        e.generation = 1;
        pack_inode_record_into_stat(*inode, e.attr);
        e.attr_timeout = 0.01;
        e.entry_timeout = 0.01;

        const size_t used = fuse_add_direntry_plus(
            req, reinterpret_cast<char *>(buf.data() + consumed), remaining,
            name_copy.c_str(), &e, next_offset);
        if (used > remaining) {
          return std::unexpected(DirentAddError::NoSpace);
        }
        consumed += used;
        next_offset += 1;
        return {};
      }

      struct stat attr{};
      attr.st_ino = entry.inode();
      attr.st_mode = entry.type();

      const size_t used = fuse_add_direntry(
          req, reinterpret_cast<char *>(buf.data() + consumed), remaining,
          name_copy.c_str(), &attr, next_offset);
      if (used > remaining) {
        return std::unexpected(DirentAddError::NoSpace);
      }
      consumed += used;
      next_offset += 1;
      return {};
    }

    [[nodiscard]] Self finish() && {
      buf.resize(consumed);
      return Self::Buf(std::move(buf));
    }

  private:
    fuse_req_t req;
    bool plus_mode;
    off_t next_offset;
    std::vector<uint8_t> buf;
    size_t consumed = 0;
  };

  static DirentCollectorSpec
  make_dirent_collector_spec(size_t max_bytes, bool plus_mode = false) {
    return DirentCollectorSpec{
        .max_bytes = max_bytes,
        .plus_mode = plus_mode,
    };
  }

  static DirentCollector make_dirent_collector(
      fuse_req_t req, off_t start_off, const DirentCollectorSpec &spec) {
    return DirentCollector(req, start_off, spec);
  }

  static bool trace_errors_enabled() {
    static const bool enabled = (getenv("FDBFS_TRACE_ERRORS") != nullptr);
    return enabled;
  }
  static bool request_interrupted(fuse_req_t req) {
    return fuse_req_interrupted(req);
  }
  static const fuse_ctx *request_ctx(fuse_req_t req) {
    return fuse_req_ctx(req);
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
  static void report_failure(InflightT<Self> *i, int err) {
    fuse_reply_err(i->req, err);
  }
  static Self BeginWait(InflightCallbackT<Self> newcb) {
    return Self(false, true, false, [newcb](InflightT<Self> *i) {
      i->attempt_state().cb.emplace(newcb);
    });
  }
  // Error surfaced from fdb_transaction_* API calls where FoundationDB expects
  // us to run fdb_transaction_on_error for retryable codes.
  static Self FDBTransactionError(
      fdb_error_t err, const char *why = "",
      std::source_location loc = std::source_location::current()) {
    trace_fdb_error("FDBTransactionError", err, why, loc);
    if (fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, err)) {
      // retryable FDB errors must flow through fdb_transaction_on_error.
      return Self(false, false, false, [](InflightT<Self> *) {}, err);
    } else {
      // can't be retried, surface an error.
      return Self(true, false, false, [](InflightT<Self> *i) {
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
  static Self
  FDBError(fdb_error_t err, const char *why = "",
           std::source_location loc = std::source_location::current()) {
    trace_fdb_error("FDBError", err, why, loc);
    return Self::Abort(EIO, why, loc);
  }
  static Self Restart() {
    // TODO someday track how many restarts we've done, so that after
    // N of them, we can switch to an abort?
    return Self(false, false, true, [](InflightT<Self> *) {});
  }
  static Self None() {
    return Self(true, false, false,
                [](InflightT<Self> *i) { fuse_reply_none(i->req); });
  };
  static Self Ignore() {
    return Self(true, false, false, [](InflightT<Self> *i) {

    });
  };
  static Self OK() {
    return Self(true, false, false,
                [](InflightT<Self> *i) { fuse_reply_err(i->req, 0); });
  };
  static Self
  Abort(int err, const char *why = "",
        std::source_location loc = std::source_location::current()) {
    trace_errno_error("Abort", err, why, loc);
    return Self(true, false, false,
                [err](InflightT<Self> *i) { fuse_reply_err(i->req, err); });
  }
  static Self Entry(struct fuse_entry_param e) {
    return Self(true, false, false, [e](InflightT<Self> *i) {
      auto generation = increment_lookup_count(e.ino);
      if (generation.has_value()) {
        // first lookup for this inode in local kernel cache; publish
        // a use record before replying to fuse.
        (new Inflight_markusedT<Self>(i->req, *generation, e,
                                      make_transaction()))
            ->start();
      } else {
        // already known locally; no new use record needed.
        fuse_reply_entry(i->req, &e);
      }
    });
  }
  static Self ImmediateEntry(
      struct fuse_entry_param e,
      std::function<void(InflightT<Self> *)> on_failure = {}) {
    return Self(true, false, false,
                [e, on_failure = std::move(on_failure)](InflightT<Self> *i) {
                  if (fuse_reply_entry(i->req, &e) < 0 && on_failure) {
                    on_failure(i);
                  }
                });
  }
  static Self Attr(struct stat attr) {
    return Self(true, false, false, [attr](InflightT<Self> *i) {
      fuse_reply_attr(i->req, &attr, 0.0);
    });
  }
  static Self Open(fuse_ino_t ino, struct fuse_file_info fi) {
    return Self(true, false, false, [ino, fi](InflightT<Self> *i) mutable {
      (void)reply_open_with_handle(i->req, ino, &fi);
    });
  }
  static Self Buf(std::vector<uint8_t> buf, int actual_size = -1) {
    // Note, per the default value for actual_size, we might receive
    // a buffer which is larger than the amount of useful/valid data
    // in it. By passing in an actual_size value, calling code can
    // restrict the amount of buf which is passed along.
    return Self(true, false, false, [buf, actual_size](InflightT<Self> *i) {
      assert(actual_size < 0 || static_cast<size_t>(actual_size) <= buf.size());
      fuse_reply_buf(i->req, reinterpret_cast<const char *>(buf.data()),
                     (actual_size >= 0) ? actual_size : buf.size());
    });
  }
  static Self Readlink(std::string name) {
    return Self(true, false, false, [name](InflightT<Self> *i) {
      fuse_reply_readlink(i->req, name.c_str());
    });
  }
  static Self Write(size_t size) {
    return Self(true, false, false,
                [size](InflightT<Self> *i) { fuse_reply_write(i->req, size); });
  }
  static Self Statfs(std::shared_ptr<struct statvfs> statbuf) {
    return Self(true, false, false, [statbuf](InflightT<Self> *i) {
      fuse_reply_statfs(i->req, statbuf.get());
    });
  }
  static Self XattrSize(ssize_t size) {
    return Self(true, false, false,
                [size](InflightT<Self> *i) { fuse_reply_xattr(i->req, size); });
  }

protected:
  FuseInflightAction(bool delete_this, bool begin_wait, bool restart,
                     std::function<void(InflightT<Self> *)> perform,
                     std::optional<fdb_error_t> retryable_err = std::nullopt)
      : delete_this(delete_this), begin_wait(begin_wait), restart(restart),
        perform(std::move(perform)), retryable_err(retryable_err) {};

  bool delete_this = false;
  bool begin_wait = false;
  bool restart = false;
  std::function<void(InflightT<Self> *)> perform;
  std::optional<fdb_error_t> retryable_err;

  friend class InflightT<Self>;
};

#endif
