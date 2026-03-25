#ifndef __SQLITE_INFLIGHT_ACTION_H__
#define __SQLITE_INFLIGHT_ACTION_H__

#include <algorithm>
#include <expected>
#include <functional>
#include <future>
#include <optional>
#include <source_location>
#include <string>
#include <string_view>
#include <sys/statvfs.h>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "generic/inflight.h"

struct SqliteReplyStatfs {
  struct statvfs statbuf{};
};

struct SqliteReplyINode {
  INodeRecord inode;
};

struct SqliteReplyBuf {
  std::vector<uint8_t> bytes;
};

struct SqliteReplyReadlink {
  std::string target;
};

struct SqliteReplyWrite {
  size_t size = 0;
};

struct SqliteReplyXattrSize {
  ssize_t size = 0;
};

struct SqliteReplyNone {};

using SqliteReply =
    std::variant<SqliteReplyStatfs, SqliteReplyINode, SqliteReplyBuf,
                 SqliteReplyReadlink, SqliteReplyWrite, SqliteReplyXattrSize,
                 SqliteReplyNone>;
using SqliteResult = std::expected<SqliteReply, int>;

struct SqliteRequest {
  std::promise<SqliteResult> promise;
};

class SqliteInflightAction {
public:
  using Self = SqliteInflightAction;
  using req_t = SqliteRequest *;
  struct INodeHandlerEntry {};
  struct INodeHandlerImmediateEntry {
    std::function<void()> failure_callback;
  };

  struct DirentCollectorSpec {};

  static std::optional<int> inode_handler_open_flags(const std::monostate &) {
    return std::nullopt;
  }

  static std::monostate inode_handler_open(int) { return {}; }

  class DirentCollector {
  public:
    [[nodiscard]] size_t estimate_remaining_entries() const { return 0; }

    [[nodiscard]] DirentAddResult try_add(std::string_view,
                                          const DirectoryEntry &,
                                          const INodeRecord * = nullptr) {
      return std::unexpected(DirentAddError::NoSpace);
    }

    [[nodiscard]] Self finish() && { return Self::None(); }
  };

  static DirentCollectorSpec make_dirent_collector_spec(size_t,
                                                        bool = false,
                                                        size_t = 0) {
    return {};
  }

  static DirentCollector make_dirent_collector(req_t, off_t,
                                               const DirentCollectorSpec &) {
    return {};
  }

  static bool trace_errors_enabled() { return false; }

  static std::string format_req(req_t req) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%p", static_cast<void *>(req));
    return std::string(buf);
  }

  static bool request_interrupted(req_t) { return false; }

  static fdbfs_request_ctx request_ctx(req_t) {
    return fdbfs_request_ctx{
        .uid = 0,
        .gid = 0,
        .pid = 0,
        .umask = 0,
    };
  }

  static void trace_errno_error(const char *, int, const char *,
                                const std::source_location &) {}

  static void trace_fdb_error(const char *, fdb_error_t, const char *,
                              const std::source_location &) {}

  static void report_failure(InflightT<Self> *i, int err) {
    i->completion_error = err;
    fulfill(i->req, std::unexpected(err));
  }

  static Self BeginWait(InflightCallbackT<Self> newcb) {
    return Self(false, true, false, [newcb](InflightT<Self> *i) {
      i->attempt_state().cb.emplace(newcb);
    });
  }

  static Self
  FDBTransactionError(fdb_error_t err, const char *why = "",
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

  static Self OK() {
    return Self(true, false, false, [](InflightT<Self> *i) {
      i->completion_error = 0;
      fulfill(i->req, SqliteReply{SqliteReplyNone{}});
    });
  }

  static Self
  Abort(int err, const char *why = "",
        std::source_location loc = std::source_location::current()) {
    trace_errno_error("Abort", err, why, loc);
    return Self(true, false, false, [err](InflightT<Self> *i) {
      i->completion_error = err;
      fulfill(i->req, std::unexpected(err));
    });
  }

  static Self Statfs(std::unique_ptr<struct statvfs> statbuf) {
    auto shared_statbuf = std::shared_ptr<struct statvfs>(std::move(statbuf));
    return Self(true, false, false,
                [statbuf = std::move(shared_statbuf)](InflightT<Self> *i) {
                  i->completion_error = 0;
                  fulfill(i->req,
                          SqliteReply{SqliteReplyStatfs{.statbuf = *statbuf}});
                });
  }

  template <typename INodeHandlerT>
  static Self INode(const INodeRecord &inode, INodeHandlerT) {
    if constexpr (std::is_same_v<INodeHandlerT, INodeHandlerEntry>) {
      return Self(true, false, false, [inode](InflightT<Self> *i) {
        auto generation = increment_lookup_count(inode.inode());
        if (generation.has_value()) {
          (new Inflight_markusedT<Self>(i->req, *generation, inode,
                                        make_transaction()))
              ->start();
        } else {
          i->completion_error = 0;
          fulfill(i->req, SqliteReply{SqliteReplyINode{.inode = inode}});
        }
      });
    } else if constexpr (std::is_same_v<INodeHandlerT,
                                        INodeHandlerImmediateEntry>) {
      return Self(true, false, false, [inode](InflightT<Self> *i) {
        i->completion_error = 0;
        fulfill(i->req, SqliteReply{SqliteReplyINode{.inode = inode}});
      });
    } else {
      return Self(true, false, false, [inode](InflightT<Self> *i) {
        i->completion_error = 0;
        fulfill(i->req, SqliteReply{SqliteReplyINode{.inode = inode}});
      });
    }
  }

  static Self None() {
    return Self(true, false, false, [](InflightT<Self> *i) {
      i->completion_error = 0;
      fulfill(i->req, SqliteReply{SqliteReplyNone{}});
    });
  }

  static Self Ignore() {
    return Self(true, false, false, [](InflightT<Self> *i) {
      i->completion_error = 0;
    });
  }

  static Self Buf(std::vector<uint8_t> buf, int actual_size = -1) {
    return Self(true, false, false,
                [buf = std::move(buf), actual_size](InflightT<Self> *i) mutable {
                  if (actual_size >= 0 &&
                      static_cast<size_t>(actual_size) <= buf.size()) {
                    buf.resize(static_cast<size_t>(actual_size));
                  }
                  i->completion_error = 0;
                  fulfill(i->req,
                          SqliteReply{SqliteReplyBuf{.bytes = std::move(buf)}});
                });
  }

  static Self Readlink(std::string name) {
    return Self(true, false, false, [name = std::move(name)](InflightT<Self> *i) {
      i->completion_error = 0;
      fulfill(i->req, SqliteReply{SqliteReplyReadlink{.target = name}});
    });
  }

  static Self Write(size_t size) {
    return Self(true, false, false, [size](InflightT<Self> *i) {
      i->completion_error = 0;
      fulfill(i->req, SqliteReply{SqliteReplyWrite{.size = size}});
    });
  }

  static Self XattrSize(ssize_t size) {
    return Self(true, false, false, [size](InflightT<Self> *i) {
      i->completion_error = 0;
      fulfill(i->req, SqliteReply{SqliteReplyXattrSize{.size = size}});
    });
  }

protected:
  SqliteInflightAction(bool delete_this, bool begin_wait, bool restart,
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

private:
  static void fulfill(req_t req, SqliteResult result) {
    if (req == nullptr) {
      return;
    }
    try {
      req->promise.set_value(std::move(result));
    } catch (const std::future_error &) {
      std::terminate();
    }
  }
};

#endif
