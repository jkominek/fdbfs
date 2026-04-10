#ifndef __NBDKIT_INFLIGHT_ACTION_H__
#define __NBDKIT_INFLIGHT_ACTION_H__

#include <expected>
#include <functional>
#include <future>
#include <optional>
#include <source_location>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "generic/inflight.h"

struct NbdkitReplyStatfs {
  struct statvfs statbuf{};
};

struct NbdkitReplyINode {
  INodeRecord inode;
  struct stat attr{};
};

struct NbdkitReplyNames {
  std::vector<std::string> names;
  std::string last_seen_name;
  size_t seen_count = 0;
};

struct NbdkitReplyBuf {
  std::vector<uint8_t> bytes;
};

struct NbdkitReplyReadlink {
  std::string target;
};

struct NbdkitReplyWrite {
  size_t size = 0;
};

struct NbdkitReplyXattrSize {
  ssize_t size = 0;
};

struct NbdkitReplyNone {};

using NbdkitReply =
    std::variant<NbdkitReplyStatfs, NbdkitReplyINode, NbdkitReplyNames,
                 NbdkitReplyBuf, NbdkitReplyReadlink, NbdkitReplyWrite,
                 NbdkitReplyXattrSize, NbdkitReplyNone>;
using NbdkitResult = std::expected<NbdkitReply, int>;

struct NbdkitRequest {
  std::promise<NbdkitResult> promise;
  std::optional<fdb_error_t> fdb_error;
};

class NbdkitInflightAction {
public:
  using Self = NbdkitInflightAction;
  using req_t = NbdkitRequest *;

  struct INodeHandlerOpen {};
  struct INodeHandlerImmediateEntry {
    std::function<void()> failure_callback;
  };

  struct DirentCollectorSpec {
    size_t estimated_entries = std::numeric_limits<size_t>::max();
  };

  static std::optional<int> inode_handler_open_flags(const INodeHandlerOpen &) {
    return std::nullopt;
  }

  static INodeHandlerOpen inode_handler_open(int) { return {}; }

  class DirentCollector {
  public:
    explicit DirentCollector(const DirentCollectorSpec &spec)
        : estimated_entries(spec.estimated_entries) {}

    [[nodiscard]] int estimate_remaining_entries() const {
      return static_cast<int>(
          std::min(estimated_entries,
                   static_cast<size_t>(std::numeric_limits<int>::max())));
    }

    [[nodiscard]] DirentAddResult try_add(std::string_view name,
                                          const DirectoryEntry &,
                                          const INodeRecord *inode = nullptr) {
      if (inode == nullptr) {
        return std::unexpected(DirentAddError::InvalidInput);
      }
      ++seen_count;
      last_seen_name.assign(name);
      if ((inode->mode() & S_IFMT) != S_IFREG) {
        return {};
      }
      names.emplace_back(name);
      return {};
    }

    [[nodiscard]] Self finish() && {
      return Self::Names(std::move(names), std::move(last_seen_name),
                         seen_count);
    }

  private:
    size_t estimated_entries;
    size_t seen_count = 0;
    std::string last_seen_name;
    std::vector<std::string> names;
  };

  static DirentCollectorSpec make_dirent_collector_spec(size_t estimated_entries,
                                                        bool = false,
                                                        size_t = 0) {
    return DirentCollectorSpec{
        .estimated_entries = estimated_entries,
    };
  }

  static DirentCollector make_dirent_collector(req_t,
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
    i->completion_error = err;
    fulfill(i->req, std::unexpected(err));
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
    return Self(true, false, false, [err](InflightT<Self> *i) {
      i->completion_error = EIO;
      if (i->req != nullptr) {
        i->req->fdb_error = err;
      }
      fulfill(i->req, std::unexpected(EIO));
    });
  }

  static Self
  FDBError(fdb_error_t err, const char *why = "",
           std::source_location loc = std::source_location::current()) {
    trace_fdb_error("FDBError", err, why, loc);
    return Self(true, false, false, [err](InflightT<Self> *i) {
      i->completion_error = EIO;
      if (i->req != nullptr) {
        i->req->fdb_error = err;
      }
      fulfill(i->req, std::unexpected(EIO));
    });
  }

  static Self Restart() {
    return Self(false, false, true, [](InflightT<Self> *) {});
  }

  static Self OK() {
    return Self(true, false, false, [](InflightT<Self> *i) {
      i->completion_error = 0;
      fulfill(i->req, NbdkitReply{NbdkitReplyNone{}});
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
                          NbdkitReply{NbdkitReplyStatfs{.statbuf = *statbuf}});
                });
  }

  template <typename INodeHandlerT>
  static Self INode(const INodeRecord &inode, INodeHandlerT) {
    if constexpr (std::is_same_v<INodeHandlerT, INodeHandlerOpen>) {
      return Self(true, false, false, [inode](InflightT<Self> *i) {
        auto generation = increment_lookup_count(inode.inode());
        if (generation.has_value()) {
          (new Inflight_markusedT<Self>(i->req, *generation, inode,
                                        make_transaction()))
              ->start();
        } else {
          struct stat attr{};
          pack_inode_record_into_stat(inode, attr);
          i->completion_error = 0;
          fulfill(i->req,
                  NbdkitReply{NbdkitReplyINode{.inode = inode, .attr = attr}});
        }
      });
    } else if constexpr (std::is_same_v<INodeHandlerT,
                                        INodeHandlerImmediateEntry>) {
      return Self(true, false, false, [inode](InflightT<Self> *i) {
        struct stat attr{};
        pack_inode_record_into_stat(inode, attr);
        i->completion_error = 0;
        fulfill(i->req,
                NbdkitReply{NbdkitReplyINode{.inode = inode, .attr = attr}});
      });
    } else {
      return Self(true, false, false, [inode](InflightT<Self> *i) {
        struct stat attr{};
        pack_inode_record_into_stat(inode, attr);
        i->completion_error = 0;
        fulfill(i->req,
                NbdkitReply{NbdkitReplyINode{.inode = inode, .attr = attr}});
      });
    }
  }

  static Self None() {
    return Self(true, false, false, [](InflightT<Self> *i) {
      i->completion_error = 0;
      fulfill(i->req, NbdkitReply{NbdkitReplyNone{}});
    });
  }

  static Self Ignore() {
    return Self(true, false, false,
                [](InflightT<Self> *i) { i->completion_error = 0; });
  }

  static Self Names(std::vector<std::string> names, std::string last_seen_name,
                    size_t seen_count) {
    return Self(true, false, false,
                [names = std::move(names), last_seen_name = std::move(last_seen_name),
                 seen_count](InflightT<Self> *i) mutable {
                  i->completion_error = 0;
                  fulfill(i->req, NbdkitReply{NbdkitReplyNames{
                                      .names = std::move(names),
                                      .last_seen_name = std::move(last_seen_name),
                                      .seen_count = seen_count}});
                });
  }

  static Self Buf(std::vector<uint8_t> buf, int actual_size = -1) {
    return Self(
        true, false, false,
        [buf = std::move(buf), actual_size](InflightT<Self> *i) mutable {
          if (actual_size >= 0 &&
              static_cast<size_t>(actual_size) <= buf.size()) {
            buf.resize(static_cast<size_t>(actual_size));
          }
          i->completion_error = 0;
          fulfill(i->req, NbdkitReply{NbdkitReplyBuf{.bytes = std::move(buf)}});
        });
  }

  static Self Readlink(std::string name) {
    return Self(true, false, false,
                [name = std::move(name)](InflightT<Self> *i) mutable {
                  i->completion_error = 0;
                  fulfill(i->req, NbdkitReply{NbdkitReplyReadlink{
                                      .target = std::move(name)}});
                });
  }

  static Self Write(size_t size) {
    return Self(true, false, false, [size](InflightT<Self> *i) {
      i->completion_error = 0;
      fulfill(i->req, NbdkitReply{NbdkitReplyWrite{.size = size}});
    });
  }

  static Self XattrSize(ssize_t size) {
    return Self(true, false, false, [size](InflightT<Self> *i) {
      i->completion_error = 0;
      fulfill(i->req, NbdkitReply{NbdkitReplyXattrSize{.size = size}});
    });
  }

protected:
  NbdkitInflightAction(bool delete_this, bool begin_wait, bool restart,
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
  static void fulfill(req_t req, NbdkitResult result) {
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
