#ifndef __FUSE_INFLIGHT_ACTION_H__
#define __FUSE_INFLIGHT_ACTION_H__

#include <string>

#include "generic/inflight.h"
#include "util_fuse.h"

class FuseInflightAction {
public:
  using Self = FuseInflightAction;
  using req_t = fuse_req_t;
  struct INodeHandlerAttr {};
  struct INodeHandlerStatxMask {
    uint32_t mask = 0;
  };
  struct INodeHandlerEntry {};
  struct INodeHandlerImmediateEntry {
    std::function<void()> failure_callback;
  };
  struct INodeHandlerOpen {
    int flags = 0;
  };
  using INodeHandler =
      std::variant<INodeHandlerAttr, INodeHandlerStatxMask, INodeHandlerEntry,
                   INodeHandlerImmediateEntry, INodeHandlerOpen>;

  // TODO these two things are kind of obnoxious, and stem from serializing
  // the open flags into the oplog. that's almost certainly unnecessary, but
  // we'll worry about cleaning that up later.
  static std::optional<int>
  inode_handler_open_flags(const INodeHandler &inode_handler) {
    auto open_handler = std::get_if<INodeHandlerOpen>(&inode_handler);
    if (open_handler == nullptr) {
      return std::nullopt;
    }
    return open_handler->flags;
  }
  static INodeHandler inode_handler_open(int flags) {
    return INodeHandlerOpen{flags};
  }

  struct DirentCollectorSpec {
    size_t max_bytes;
    bool plus_mode;
  };

  class DirentCollector {
  public:
    DirentCollector(req_t req, off_t start_off, const DirentCollectorSpec &spec)
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

    [[nodiscard]] DirentAddResult try_add(std::string_view name,
                                          const DirectoryEntry &entry,
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
    req_t req;
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

  static DirentCollector
  make_dirent_collector(req_t req, off_t start_off,
                        const DirentCollectorSpec &spec) {
    return DirentCollector(req, start_off, spec);
  }

  static bool trace_errors_enabled() {
    static const bool enabled = (getenv("FDBFS_TRACE_ERRORS") != nullptr);
    return enabled;
  }
  static std::string format_req(req_t req) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%p", static_cast<void *>(req));
    return std::string(buf);
  }
  static bool request_interrupted(req_t req) {
    return fuse_req_interrupted(req);
  }
  static fdbfs_request_ctx request_ctx(req_t req) {
    const fuse_ctx *ctx = fuse_req_ctx(req);
    return fdbfs_request_ctx{
        .uid = ctx->uid,
        .gid = ctx->gid,
        .pid = ctx->pid,
        .umask = ctx->umask,
    };
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
    i->completion_error = err;
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
        i->completion_error = EIO;
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
    return Self(true, false, false, [](InflightT<Self> *i) {
      i->completion_error = 0;
      fuse_reply_none(i->req);
    });
  };
  static Self Ignore() {
    return Self(true, false, false, [](InflightT<Self> *i) {

    });
  };
  static Self OK() {
    return Self(true, false, false, [](InflightT<Self> *i) {
      i->completion_error = 0;
      fuse_reply_err(i->req, 0);
    });
  };
  static Self
  Abort(int err, const char *why = "",
        std::source_location loc = std::source_location::current()) {
    trace_errno_error("Abort", err, why, loc);
    return Self(true, false, false, [err](InflightT<Self> *i) {
      i->completion_error = err;
      fuse_reply_err(i->req, err);
    });
  }
  static Self INode(const INodeRecord &inode, INodeHandler selector) {
    return std::visit(
        [&](const auto &selected) -> Self {
          using T = std::decay_t<decltype(selected)>;
          if constexpr (std::is_same_v<T, INodeHandlerAttr>) {
            struct stat attr{};
            pack_inode_record_into_stat(inode, attr);
            return Self(true, false, false, [attr](InflightT<Self> *i) {
              fuse_reply_attr(i->req, &attr, 0.0);
            });
          } else if constexpr (std::is_same_v<T, INodeHandlerStatxMask>) {
            // TODO unimplemented path
            (void)selected.mask;
            return Self(true, false, false, [](InflightT<Self> *i) {
              fuse_reply_err(i->req, ENOSYS);
            });
          } else if constexpr (std::is_same_v<T, INodeHandlerEntry>) {
            struct stat attr{};
            pack_inode_record_into_stat(inode, attr);
            return Self(true, false, false, [attr, inode](InflightT<Self> *i) {
              auto generation =
                  increment_lookup_count(static_cast<fdbfs_ino_t>(attr.st_ino));
              if (generation.has_value()) {
                // first lookup for this inode in local kernel cache; publish
                // a use record before replying to fuse.
                (new Inflight_markusedT<Self>(i->req, *generation, inode,
                                              make_transaction()))
                    ->start();
              } else {
                // already known locally; no new use record needed.
                struct fuse_entry_param e{};
                e.ino = static_cast<fuse_ino_t>(attr.st_ino);
                e.generation = 1;
                e.attr = attr;
                e.attr_timeout = 0.01;
                e.entry_timeout = 0.01;
                i->completion_error = 0;
                fuse_reply_entry(i->req, &e);
              }
            });
          } else if constexpr (std::is_same_v<T, INodeHandlerImmediateEntry>) {
            struct stat attr{};
            pack_inode_record_into_stat(inode, attr);
            return Self(
                true, false, false,
                [attr, failure_callback =
                           selected.failure_callback](InflightT<Self> *i) {
                  struct fuse_entry_param e{};
                  e.ino = static_cast<fuse_ino_t>(attr.st_ino);
                  e.generation = 1;
                  e.attr = attr;
                  e.attr_timeout = 0.01;
                  e.entry_timeout = 0.01;
                  i->completion_error = 0;
                  if (fuse_reply_entry(i->req, &e) < 0 && failure_callback) {
                    failure_callback();
                  }
                });
          } else if constexpr (std::is_same_v<T, INodeHandlerOpen>) {
            return Self(true, false, false,
                        [ino = inode.inode(),
                         flags = selected.flags](InflightT<Self> *i) mutable {
                          struct fuse_file_info fi{};
                          fi.flags = flags;
                          i->completion_error = 0;
                          (void)reply_open_with_handle(
                              i->req, static_cast<fuse_ino_t>(ino), &fi);
                        });
          }
        },
        selector);
  }
  static Self Buf(std::vector<uint8_t> buf, int actual_size = -1) {
    // Note, per the default value for actual_size, we might receive
    // a buffer which is larger than the amount of useful/valid data
    // in it. By passing in an actual_size value, calling code can
    // restrict the amount of buf which is passed along.
    return Self(true, false, false, [buf, actual_size](InflightT<Self> *i) {
      assert(actual_size < 0 || static_cast<size_t>(actual_size) <= buf.size());
      i->completion_error = 0;
      fuse_reply_buf(i->req, reinterpret_cast<const char *>(buf.data()),
                     (actual_size >= 0) ? actual_size : buf.size());
    });
  }
  static Self Readlink(std::string name) {
    return Self(true, false, false, [name](InflightT<Self> *i) {
      i->completion_error = 0;
      fuse_reply_readlink(i->req, name.c_str());
    });
  }
  static Self Write(size_t size) {
    return Self(true, false, false, [size](InflightT<Self> *i) {
      i->completion_error = 0;
      fuse_reply_write(i->req, size);
    });
  }
  static Self Statfs(std::shared_ptr<struct statvfs> statbuf) {
    return Self(true, false, false, [statbuf](InflightT<Self> *i) {
      i->completion_error = 0;
      fuse_reply_statfs(i->req, statbuf.get());
    });
  }
  static Self XattrSize(ssize_t size) {
    return Self(true, false, false, [size](InflightT<Self> *i) {
      i->completion_error = 0;
      fuse_reply_xattr(i->req, size);
    });
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
