#ifndef __VOID_INFLIGHT_ACTION_H__
#define __VOID_INFLIGHT_ACTION_H__

#include <cstdlib>
#include <functional>
#include <optional>
#include <source_location>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "inflight.h"

class VoidInflightAction {
public:
  using Self = VoidInflightAction;
  using req_t = std::monostate;

  static std::optional<int>
  inode_handler_open_flags(const std::monostate &inode_handler) {
    return std::nullopt;
  }

  static std::monostate inode_handler_open(int flags) { return {}; }

  struct DirentCollectorSpec {
    size_t max_entries = 64;
    bool plus_mode = false;
  };

  class DirentCollector {
  public:
    explicit DirentCollector(const DirentCollectorSpec &spec)
        : plus_mode_(spec.plus_mode), max_entries_(spec.max_entries) {}

    [[nodiscard]] size_t estimate_remaining_entries() const {
      if (count_ >= max_entries_) {
        return 0;
      }
      return max_entries_ - count_;
    }

    [[nodiscard]] DirentAddResult try_add(std::string_view,
                                          const DirectoryEntry &,
                                          const INodeRecord *inode = nullptr) {
      if (count_ >= max_entries_) {
        return std::unexpected(DirentAddError::NoSpace);
      }
      if (plus_mode_ && (inode == nullptr)) {
        return std::unexpected(DirentAddError::InvalidInput);
      }
      count_ += 1;
      return {};
    }

    [[nodiscard]] Self finish() && { return Self::Buf({}); }

  private:
    bool plus_mode_ = false;
    size_t max_entries_ = 0;
    size_t count_ = 0;
  };

  static DirentCollectorSpec
  make_dirent_collector_spec(size_t max_bytes, bool plus_mode = false,
                             size_t max_entries = 0) {
    size_t derived_cap = max_entries;
    if (derived_cap == 0) {
      derived_cap = std::max<size_t>(1, max_bytes / 64);
    }
    return DirentCollectorSpec{
        .max_entries = derived_cap,
        .plus_mode = plus_mode,
    };
  }

  static DirentCollector
  make_dirent_collector(req_t, const DirentCollectorSpec &spec) {
    return DirentCollector(spec);
  }

  static bool trace_errors_enabled() {
    static const bool enabled = (getenv("FDBFS_TRACE_ERRORS") != nullptr);
    return enabled;
  }

  static std::string format_req(req_t) { return "<void>"; }

  static bool request_interrupted(req_t) { return false; }

  static fdbfs_request_ctx request_ctx(req_t) {
    return fdbfs_request_ctx{
        .uid = 0,
        .gid = 0,
        .pid = 0,
        .umask = 0,
    };
  }

  static void trace_errno_error(const char *kind, int err, const char *why,
                                const std::source_location &loc) {
    if (!trace_errors_enabled()) {
      return;
    }
    fprintf(stderr, "fdbfs %s: err=%d (%s) at %s:%u (%s)%s%s\n", kind, err,
            strerror(err), loc.file_name(), loc.line(), loc.function_name(),
            (why && why[0]) ? " why=" : "", (why && why[0]) ? why : "");
  }

  static void trace_fdb_error(const char *kind, fdb_error_t err,
                              const char *why,
                              const std::source_location &loc) {
    if (!trace_errors_enabled()) {
      return;
    }
    fprintf(stderr, "fdbfs %s: fdb_err=%d (%s) at %s:%u (%s)%s%s\n", kind, err,
            fdb_get_error(err), loc.file_name(), loc.line(),
            loc.function_name(), (why && why[0]) ? " why=" : "",
            (why && why[0]) ? why : "");
  }

  static void report_failure(InflightT<Self> *i, int err) {
    i->completion_error = err;
  }

  static Self BeginWait(InflightCallbackT<Self> newcb) {
    return Self(false, true, false, [newcb](InflightT<Self> *i) {
      i->attempt_state().cb.emplace(newcb);
    });
  }

  static Self FDBTransactionError(
      fdb_error_t err, const char *why = "",
      std::source_location loc = std::source_location::current()) {
    trace_fdb_error("FDBTransactionError", err, why, loc);
    if (fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, err)) {
      return Self(false, false, false, [](InflightT<Self> *) {}, err);
    }
    return Abort(EIO, why, loc);
  }

  static Self
  FDBError(fdb_error_t err, const char *why = "",
           std::source_location loc = std::source_location::current()) {
    trace_fdb_error("FDBError", err, why, loc);
    return Abort(EIO, why, loc);
  }

  static Self Restart() {
    return Self(false, false, true, [](InflightT<Self> *) {});
  }

  static Self None() {
    return Self(true, false, false,
                [](InflightT<Self> *i) { i->completion_error = 0; });
  }

  static Self Ignore() { return None(); }

  static Self OK() { return None(); }

  static Self
  Abort(int err, const char *why = "",
        std::source_location loc = std::source_location::current()) {
    trace_errno_error("Abort", err, why, loc);
    return Self(true, false, false,
                [err](InflightT<Self> *i) { i->completion_error = err; });
  }

  static Self Statfs(std::unique_ptr<struct statvfs>) { return None(); }

  template <typename INodeHandlerT>
  static Self INode(const INodeRecord &, INodeHandlerT) {
    return None();
  }

  static Self Buf(std::vector<uint8_t>, int = -1) { return None(); }

  static Self Readlink(std::string) { return None(); }

  static Self Write(size_t) { return None(); }

  static Self XattrSize(ssize_t) { return None(); }

protected:
  VoidInflightAction(bool delete_this, bool begin_wait, bool restart,
                     std::function<void(InflightT<Self> *)> perform,
                     std::optional<fdb_error_t> retryable_err = std::nullopt)
      : delete_this(delete_this), begin_wait(begin_wait), restart(restart),
        perform(std::move(perform)), retryable_err(retryable_err) {}

  bool delete_this = false;
  bool begin_wait = false;
  bool restart = false;
  std::function<void(InflightT<Self> *)> perform;
  std::optional<fdb_error_t> retryable_err;

  friend class InflightT<Self>;
};

#endif
