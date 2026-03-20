#pragma once

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <expected>
#include <future>
#include <memory>
#include <optional>
#include <source_location>
#include <string>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "generic/inflight.h"

struct TestReplyStatfs {
  struct statvfs statbuf{};
};

struct TestReplyINode {
  INodeRecord inode;
};

struct TestReplyBuf {
  std::vector<uint8_t> bytes;
};

struct TestReplyReadlink {
  std::string target;
};

struct TestReplyWrite {
  size_t size = 0;
};

struct TestReplyXattrSize {
  ssize_t size = 0;
};

struct TestDirentRecord {
  std::string name;
  DirectoryEntry dirent;
  std::optional<INodeRecord> inode;
};

struct TestReplyDirents {
  bool plus_mode = false;
  std::vector<TestDirentRecord> entries;
};

struct TestReplyNone {};

using TestReply =
    std::variant<TestReplyStatfs, TestReplyINode, TestReplyBuf,
                 TestReplyReadlink, TestReplyWrite, TestReplyXattrSize,
                 TestReplyDirents, TestReplyNone>;
using TestResult = std::expected<TestReply, int>;

struct TestRequest {
  std::promise<TestResult> promise;
};

class TestInflightAction {
public:
  using Self = TestInflightAction;
  using req_t = TestRequest *;
  struct INodeHandlerEntry {};
  struct INodeHandlerImmediateEntry {
    std::function<void()> failure_callback;
  };
  struct DirentCollectorSpec {
    size_t max_entries = 64;
    bool plus_mode = false;
  };

  class DirentCollector {
  public:
    explicit DirentCollector(const DirentCollectorSpec &spec)
        : plus_mode_(spec.plus_mode), max_entries_(spec.max_entries) {}

    [[nodiscard]] size_t estimate_remaining_entries() const {
      if (entries.size() >= max_entries_) {
        return 0;
      }
      return max_entries_ - entries.size();
    }

    [[nodiscard]] DirentAddResult try_add(std::string_view name,
                                          const DirectoryEntry &entry,
                                          const INodeRecord *inode = nullptr) {
      if (entries.size() >= max_entries_) {
        return std::unexpected(DirentAddError::NoSpace);
      }
      if (plus_mode_ && (inode == nullptr)) {
        return std::unexpected(DirentAddError::InvalidInput);
      }

      TestDirentRecord record{
          .name = std::string(name),
          .dirent = entry,
          .inode = inode ? std::optional<INodeRecord>(*inode) : std::nullopt,
      };
      entries.push_back(std::move(record));
      return {};
    }

    [[nodiscard]] Self finish() && {
      return Self(true, false, false,
                  [plus_mode = plus_mode_,
                   records = std::move(entries)](InflightT<Self> *i) mutable {
                    fulfill(i->req, TestReply(TestReplyDirents{
                                        .plus_mode = plus_mode,
                                        .entries = std::move(records),
                                    }));
                  });
    }

  private:
    bool plus_mode_ = false;
    size_t max_entries_ = 0;
    std::vector<TestDirentRecord> entries;
  };

  static DirentCollectorSpec
  make_dirent_collector_spec(size_t max_bytes, bool plus_mode = false,
                             size_t max_entries = 0) {
    size_t derived_cap = max_entries;
    if (derived_cap == 0) {
      // Logical collector: use an artificial per-request entry cap while
      // still scaling loosely with the caller's byte budget.
      derived_cap = std::max<size_t>(1, max_bytes / 64);
    }
    return DirentCollectorSpec{
        .max_entries = derived_cap,
        .plus_mode = plus_mode,
    };
  }

  static DirentCollector make_dirent_collector(req_t, off_t,
                                               const DirentCollectorSpec &spec) {
    return DirentCollector(spec);
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

  static Self Restart() { return Self(false, false, true, [](InflightT<Self> *) {}); }

  static Self OK() {
    return Self(true, false, false,
                [](InflightT<Self> *i) { fulfill(i->req, TestReply{TestReplyNone{}}); });
  }

  static Self
  Abort(int err, const char *why = "",
        std::source_location loc = std::source_location::current()) {
    trace_errno_error("Abort", err, why, loc);
    return Self(true, false, false,
                [err](InflightT<Self> *i) { fulfill(i->req, std::unexpected(err)); });
  }

  static Self Statfs(std::unique_ptr<struct statvfs> statbuf) {
    auto shared_statbuf = std::shared_ptr<struct statvfs>(std::move(statbuf));
    return Self(true, false, false,
                [statbuf = std::move(shared_statbuf)](InflightT<Self> *i) {
                  fulfill(i->req, TestReply{TestReplyStatfs{.statbuf = *statbuf}});
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
          fulfill(i->req, TestReply{TestReplyINode{.inode = inode}});
        }
      });
    } else if constexpr (std::is_same_v<INodeHandlerT,
                                        INodeHandlerImmediateEntry>) {
      return Self(true, false, false, [inode](InflightT<Self> *i) {
        fulfill(i->req, TestReply{TestReplyINode{.inode = inode}});
      });
    } else {
      return Self(true, false, false, [inode](InflightT<Self> *i) {
        fulfill(i->req, TestReply{TestReplyINode{.inode = inode}});
      });
    }
  }

  static Self None() {
    return Self(true, false, false,
                [](InflightT<Self> *i) { fulfill(i->req, TestReply{TestReplyNone{}}); });
  }

  static Self Ignore() {
    return Self(true, false, false, [](InflightT<Self> *) {});
  }

  static Self Buf(std::vector<uint8_t> buf, int actual_size = -1) {
    return Self(true, false, false,
                [buf = std::move(buf), actual_size](InflightT<Self> *i) mutable {
                  if (actual_size >= 0) {
                    REQUIRE(static_cast<size_t>(actual_size) <= buf.size());
                    buf.resize(static_cast<size_t>(actual_size));
                  }
                  fulfill(i->req, TestReply{TestReplyBuf{.bytes = std::move(buf)}});
                });
  }

  static Self Readlink(std::string name) {
    return Self(true, false, false,
                [name = std::move(name)](InflightT<Self> *i) {
                  fulfill(i->req,
                          TestReply{TestReplyReadlink{.target = name}});
                });
  }

  static Self Write(size_t size) {
    return Self(true, false, false,
                [size](InflightT<Self> *i) {
                  fulfill(i->req, TestReply{TestReplyWrite{.size = size}});
                });
  }

  static Self XattrSize(ssize_t size) {
    return Self(true, false, false,
                [size](InflightT<Self> *i) {
                  fulfill(i->req,
                          TestReply{TestReplyXattrSize{.size = size}});
                });
  }

protected:
  TestInflightAction(bool delete_this, bool begin_wait, bool restart,
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
  static void fulfill(req_t req, TestResult result) {
    try {
      req->promise.set_value(std::move(result));
    } catch (const std::future_error &) {
      std::terminate();
    }
  }
};
