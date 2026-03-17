#pragma once

#include <catch2/catch_test_macros.hpp>

#include <expected>
#include <future>
#include <memory>
#include <optional>
#include <source_location>
#include <string>
#include <sys/statvfs.h>
#include <utility>
#include <variant>

#include "generic/inflight.h"

struct TestReplyStatfs {
  struct statvfs statbuf{};
};

struct TestReplyINode {
  INodeRecord inode;
};

using TestReply = std::variant<TestReplyStatfs, TestReplyINode>;
using TestResult = std::expected<TestReply, int>;

struct TestRequest {
  std::promise<TestResult> promise;
};

class TestInflightAction {
public:
  using Self = TestInflightAction;
  using req_t = TestRequest *;

  static bool trace_errors_enabled() { return false; }
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
    return Self(true, false, false, [inode](InflightT<Self> *i) {
      fulfill(i->req, TestReply{TestReplyINode{.inode = inode}});
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
