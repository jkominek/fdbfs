#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>

#include "liveness.h"
#include "oplog.h"

/******************************************************
 * This encapsulates all of our retry logic
 *
 * Every sub-struct of fdbfs_inflight_base will represent
 * an async fuse request that we're in the process of handling.
 *
 * cb will point to the function which will handle the final
 * processing of the result, and send a result back to fuse.
 * if there's only one future being used, it will be called
 * only once. with multiple futures, it might send later futures
 * back to the error_checker.
 *
 * issuer takes a filled out struct and issues the FDB calls
 * to start things. the relevant future is set to fdbfs_error_checker.
 * it may be called over and over by the error_processor.
 *
 * fdbfs_error_checker checks the future for an error. if things are
 * ok, it passes along to cb. if there has been a problem, it uses
 * fdb_transaction_on_error and sets its callback to fdbfs_error_processor.
 *
 * fdbfs_error_processor will abort the fuse request if there was a
 * failure. otherwise it will use issuer to start the transaction over
 * again.
 */

// allocate an inflight struct and fill and out some basics.
template <typename ActionT>
InflightT<ActionT>::InflightT(fuse_req_t req,
                              const InflightRuntimePolicy &runtime_policy,
                              unique_transaction provided)
    : transaction(std::move(provided)), req(req), policy(&runtime_policy) {
  if (uses_oplog()) {
    op_id = allocate_oplog_id();
    mark_oplog_live(*op_id);
  }
  // we need to be more clever about this. having every single
  // operation fetch a read version is going to add a lot of latency.
#if DEBUG
  clock_gettime(CLOCK_MONOTONIC, &clockstart);
#endif
}

template <typename ActionT>
AttemptStateT<ActionT> &InflightT<ActionT>::attempt_state() {
  return *attempt;
}

template <typename ActionT>
const AttemptStateT<ActionT> &InflightT<ActionT>::attempt_state() const {
  return *attempt;
}

template <typename ActionT> void InflightT<ActionT>::reset_attempt_state() {
  attempt = create_attempt_state();
}

template <typename ActionT>
void InflightT<ActionT>::track_inode_for_fsync(fuse_ino_t ino) {
  if (fsync_tokens.find(ino) != fsync_tokens.end()) {
    return;
  }
  fsync_tokens.emplace(ino, g_fsync_barrier_table.begin_op(ino));
}

template <typename ActionT>
ActionT InflightT<ActionT>::oplog_recovery(const OpLogRecord &) {
  // it shouldn't be possible to reach this code, anything that
  // opts into oplog usage must override this.
  // TODO enforce in the typesystem, by having separate Inflight
  // subclasses for each ReadWrite value? (and then eliminating
  // the value)
  return ActionT::Abort(EIO);
}

template <typename ActionT>
bool InflightT<ActionT>::write_oplog_record(OpLogRecord record) {
  if (!uses_oplog()) {
    return true;
  }
  if (!op_id.has_value()) {
    return false;
  }
  record.set_op_id(*op_id);
  if (record.result_case() == OpLogRecord::RESULT_NOT_SET) {
    return false;
  }
  return static_cast<bool>(
      fdb_set_protobuf(transaction.get(), pack_oplog_key(pid, *op_id), record));
}

template <typename ActionT>
bool InflightT<ActionT>::write_oplog_result(const OpLogResultVariant &result) {
  OpLogRecord record;

  std::visit(
      [&record](const auto &typed_result) {
        using T = std::decay_t<decltype(typed_result)>;
        if constexpr (std::is_same_v<T, OpLogResultOK>) {
          *record.mutable_ok() = typed_result;
        } else if constexpr (std::is_same_v<T, OpLogResultEntry>) {
          *record.mutable_entry() = typed_result;
        } else if constexpr (std::is_same_v<T, OpLogResultAttr>) {
          *record.mutable_attr() = typed_result;
        } else if constexpr (std::is_same_v<T, OpLogResultOpen>) {
          *record.mutable_open() = typed_result;
        } else if constexpr (std::is_same_v<T, OpLogResultBuf>) {
          *record.mutable_buf() = typed_result;
        } else if constexpr (std::is_same_v<T, OpLogResultReadlink>) {
          *record.mutable_readlink() = typed_result;
        } else if constexpr (std::is_same_v<T, OpLogResultWrite>) {
          *record.mutable_write() = typed_result;
        } else if constexpr (std::is_same_v<T, OpLogResultStatfs>) {
          *record.mutable_statfs() = typed_result;
        } else if constexpr (std::is_same_v<T, OpLogResultXattrSize>) {
          *record.mutable_xattr_size() = typed_result;
        }
      },
      result);

  return write_oplog_record(std::move(record));
}

template <typename ActionT> void InflightT<ActionT>::cleanup() {
  if (op_id.has_value()) {
    mark_oplog_dead(*op_id);
  }
  for (auto &[ino, token] : fsync_tokens) {
    (void)ino;
    g_fsync_barrier_table.end_op(token);
  }
  fsync_tokens.clear();
#if DEBUG
  struct timespec stop;
  clock_gettime(CLOCK_MONOTONIC, &stop);
  time_t secs = (stop.tv_sec - clockstart.tv_sec);
  long nsecs = (stop.tv_nsec - clockstart.tv_nsec);
  if (secs < 5) {
    nsecs += secs * 1000000000;
    printf("inflight %p for req %p took %li ns\n", this, req, nsecs);
  } else {
    printf("inflight %p for req %p took %li s\n", this, req, secs);
  }
#endif
  if (on_done) {
    auto callback = std::move(on_done);
    callback();
  }
}

template <typename ActionT>
void InflightT<ActionT>::set_on_done(std::function<void()> callback) {
  on_done = std::move(callback);
}

template <typename ActionT>
bool InflightT<ActionT>::should_check_oplog() const {
  return uses_oplog() && commit_unknown_seen;
}

template <typename ActionT> bool InflightT<ActionT>::uses_oplog() const {
  return policy->uses_oplog;
}

template <typename ActionT> ActionT InflightT<ActionT>::check_oplog_or_issue() {
  fdb_bool_t present = 0;
  const uint8_t *val = nullptr;
  int vallen = 0;
  const fdb_error_t err = fdb_future_get_value(
      attempt_state().oplog_fetch.get(), &present, &val, &vallen);
  if (err) {
    return ActionT::FDBError(err);
  }

  if (!present) {
    // no prior committed result found, continue with normal retry path.
    InflightCallbackT<ActionT> next_cb = issue();
    if (attempt_state().future_queue.empty()) {
      return next_cb();
    }
    return ActionT::BeginWait(next_cb);
  }

  OpLogRecord record;
  if (!record.ParseFromArray(val, vallen) || !record.IsInitialized()) {
    return ActionT::Abort(EIO);
  }
  if (!op_id.has_value() || record.op_id() != *op_id) {
    return ActionT::Abort(EIO);
  }
  if (record.result_case() == OpLogRecord::RESULT_NOT_SET) {
    return ActionT::Abort(EIO);
  }

  return oplog_recovery(record);
}

template <typename ActionT> void InflightT<ActionT>::start() {
  if (!attempt) {
    reset_attempt_state();
  }

  if (shut_it_down_forever) {
    // abort everything immediately
    this->fail(ENOSYS, "fdbfs commanded to shutdown");
    return;
  }

  if (const fdb_error_t err = configure_transaction()) {
    if (fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, err)) {
      (void)retry_with_on_error(err, "configure_transaction");
    } else {
      ActionT::trace_fdb_error("ConfigureTransaction", err,
                               "configure_transaction failed",
                               std::source_location::current());
      fail(EIO, "configure_transaction failed");
    }
    return;
  }

  if (should_check_oplog()) {
    if (!op_id.has_value()) {
      // this should be impossible
      fail(EIO, "oplog check requested without op_id");
      return;
    }
    const auto key = pack_oplog_key(pid, *op_id);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        attempt_state().oplog_fetch);
    attempt_state().cb.emplace(
        std::bind(&InflightT<ActionT>::check_oplog_or_issue, this));
  } else {
    attempt_state().cb.emplace(issue());
  }

  if (attempt_state().future_queue.empty()) {
    run_current_callback();
    return;
  }
  begin_wait();
}

template <typename ActionT>
bool InflightT<ActionT>::retry_with_on_error(fdb_error_t err,
                                             const char *source) {
  // we're here because some fdb function in an Inflight subclass
  // returned an fdb_error_t, then FDB said it was a retryable error, so now
  // we've got to run on_error to either 1) wait (fdb controlled backoff) or 2)
  // perhaps permanently fail
  if (ActionT::trace_errors_enabled()) {
    char op_id_buf[32];
    fprintf(stderr,
            "fdbfs OnErrorBegin: inflight=%p req=%p type=%s op_id=%s "
            "source=%s err=%d (%s)\n",
            static_cast<void *>(this), static_cast<void *>(req),
            typeid(*this).name(),
            op_id_to_cstr(op_id, op_id_buf, sizeof(op_id_buf)), source, err,
            fdb_get_error(err));
  }

  if (uses_oplog() &&
      fdb_error_predicate(FDB_ERROR_PREDICATE_MAYBE_COMMITTED, err)) {
    commit_unknown_seen = true;
  }

  FDBFuture *nextf = fdb_transaction_on_error(transaction.get(), err);
  if (nextf == nullptr) {
    fail(EIO, "fdb_transaction_on_error returned nullptr");
    return false;
  }

  reset_attempt_state();
  if (fdb_future_set_callback(nextf, &InflightT<ActionT>::error_processor,
                              static_cast<void *>(this))) {
    // Avoid leaking the on_error future when callback setup fails.
    fdb_future_destroy(nextf);
    fail(EIO, "fdb_future_set_callback returned an error");
    return false;
  }

  return true;
}

template <typename ActionT>
ActionT InflightT<ActionT>::commit(InflightCallbackT<ActionT> cb) {
  wait_on_future(fdb_transaction_commit(transaction.get()),
                 attempt_state().commit);
  return ActionT::BeginWait(cb);
}

template <typename ActionT> bool InflightT<ActionT>::run_current_callback() {
  if (bool(attempt_state().cb)) {
    std::function<ActionT()> f = attempt_state().cb.value();
    attempt_state().cb = std::nullopt;
    ActionT a = f();
    a.perform(this);

    if (a.retryable_err.has_value()) {
      if (!retry_with_on_error(*a.retryable_err, "InflightAction")) {
        return false;
      }
      return false;
    }

    if (a.begin_wait) {
      begin_wait();
    }

    if (a.restart) {
      fdb_transaction_reset(transaction.get());
      reset_attempt_state();
      start();
      return false;
    }

    if (a.delete_this) {
      // we're dead and done, for whatever reason
      cleanup();
      delete this;
      return false;
    }

    return true;
  }

  std::cout << "no callback was set for " << typeid(*this).name()
            << " when we needed one" << std::endl;
  fail(EIO, "no callback was set when we needed one");
  return false;
}

template <typename ActionT>
void InflightT<ActionT>::future_ready(FDBFuture *f) {
  if (ActionT::request_interrupted(req)) {
    fail(EINTR, "fuse operation interrupted");
    return;
  }

  if (shut_it_down_forever) {
    fail(ENOTCONN, "fdbfs commanded to shutdown");
    return;
  }

  assert(!attempt_state().future_queue.empty());
  if (attempt_state().future_queue.empty()) {
    fail(EIO, "future_ready called with empty queue");
    return;
  }

  // only the first future should call us
  FDBFuture *next = attempt_state().future_queue.front();
  attempt_state().future_queue.pop();
  assert(next == f);
  if (next != f) {
    fail(EIO, "future_ready called for unexpected future");
    return;
  }

  if (attempt_state().future_queue.empty()) {
    run_current_callback();
    return;
  } else {
    // there are still futures we need to wait on before we
    // can return to processing the transaction, so enqueue
    // things again.
    begin_wait();
  }
}

template <typename ActionT> void InflightT<ActionT>::begin_wait() {
  if (attempt_state().future_queue.empty()) {
    // we could try to resume processing the transaction, but really this
    // just shouldn't be possible, so i'm inclined to bail on it.
    fail(EIO, "tried to start waiting on empty future queue");
    return;
  }
  if (fdb_future_set_callback(attempt_state().future_queue.front(),
                              &InflightT<ActionT>::error_checker,
                              static_cast<void *>(this))) {
    // we don't have a way to deal with this, so destroy the
    // transaction so that nothing leaks. ideally we'd notify
    // fuse at this point that the operation is dead.
    fail(EIO, "failed to set future callback");
    return;
  }
}

template <typename ActionT>
void InflightT<ActionT>::wait_on_future(FDBFuture *f, unique_future &dest) {
  attempt_state().future_queue.push(f);
  dest.reset(f);
}

template <typename ActionT>
void InflightT<ActionT>::fail(int err, const char *why,
                              std::source_location loc) {
  if (err != 0) {
    ActionT::trace_errno_error("FailInflight", err, why, loc);
  }
  if (!suppress_errors) {
    ActionT::report_failure(this, err);
  }
  cleanup();
  delete this;
}

template <typename ActionT>
void InflightT<ActionT>::error_processor(FDBFuture *f, void *p) {
  auto *inflight = static_cast<InflightT<ActionT> *>(p);

  fdb_error_t err = fdb_future_get_error(f);
  // done with this either way.
  fdb_future_destroy(f);

  if (err) {
    if (ActionT::trace_errors_enabled()) {
      char op_id_buf[32];
      fprintf(stderr,
              "fdbfs ErrorProcessorBegin: inflight=%p req=%p type=%s "
              "op_id=%s future=%p err=%d (%s)\n",
              static_cast<void *>(inflight), static_cast<void *>(inflight->req),
              typeid(*inflight).name(),
              op_id_to_cstr(inflight->op_id, op_id_buf, sizeof(op_id_buf)),
              static_cast<void *>(f), err, fdb_get_error(err));
    }
    // error during an error. foundationdb says that means
    // you should give up. so we'll let fuse know they're hosed.
    inflight->fail(EIO, "fdb_transaction_on_error future failed");
    return;
  }

  // foundationdb, perhaps after some delay, has given us the
  // goahead to start up the new transaction.
  fdb_transaction_reset(inflight->transaction.get());
  inflight->start();
}

template <typename ActionT>
void InflightT<ActionT>::error_checker(FDBFuture *f, void *p) {
  auto *inflight = static_cast<InflightT<ActionT> *>(p);

  fdb_error_t err = fdb_future_get_error(f);

  if (err) {
    if (ActionT::trace_errors_enabled()) {
      char op_id_buf[32];
      fprintf(stderr,
              "fdbfs CheckerError: inflight=%p req=%p type=%s op_id=%s "
              "future=%p err=%d (%s)\n",
              static_cast<void *>(inflight), static_cast<void *>(inflight->req),
              typeid(*inflight).name(),
              op_id_to_cstr(inflight->op_id, op_id_buf, sizeof(op_id_buf)),
              static_cast<void *>(f), err, fdb_get_error(err));
    }
    // got an error during normal processing. foundationdb says
    // we should call _on_error on it, and maybe we'll get to
    // try again, and maybe we won't.
    (void)inflight->retry_with_on_error(err, "fdbfs_error_checker");
    return;
  }

  // and finally toss the next bit of work onto the thread pool queue
  pool.push_task([inflight, f]() { inflight->future_ready(f); });
}

// Inflight_markusedT

template <typename ActionT>
Inflight_markusedT<ActionT>::Inflight_markusedT(fuse_req_t req,
                                                uint64_t generation,
                                                struct fuse_entry_param entry,
                                                unique_transaction transaction)
    : InflightWithAttemptT<AttemptState_markusedT<ActionT>,
                           InflightPolicyIdempotentWrite, ActionT>(
          req, std::move(transaction)),
      generation(generation), entry(entry) {}

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_markusedT<ActionT>::issue() {
  auto inode_key = pack_inode_key(entry.ino);
  this->wait_on_future(fdb_transaction_get(this->transaction.get(),
                                           inode_key.data(), inode_key.size(),
                                           0),
                       this->a().inode_fetch);
  return std::bind(&Inflight_markusedT<ActionT>::check_inode, this);
}

template <typename ActionT> ActionT Inflight_markusedT<ActionT>::check_inode() {
  fdb_bool_t present = 0;
  const uint8_t *val = nullptr;
  int vallen = 0;
  fdb_error_t err = fdb_future_get_value(this->a().inode_fetch.get(), &present,
                                         &val, &vallen);
  if (err)
    return ActionT::FDBError(err);

  if (!present) {
    (void)decrement_lookup_count(entry.ino, 1);
    return ActionT::Abort(EIO);
  }

  auto use_key = pack_inode_use_key(entry.ino);
  const uint64_t generation_le = htole64(generation);
  fdb_transaction_atomic_op(this->transaction.get(), use_key.data(),
                            use_key.size(),
                            reinterpret_cast<const uint8_t *>(&generation_le),
                            sizeof(generation_le), FDB_MUTATION_TYPE_MAX);
  return this->commit(
      std::bind(&Inflight_markusedT<ActionT>::reply_entry, this));
}

template <typename ActionT> ActionT Inflight_markusedT<ActionT>::reply_entry() {
  return ActionT::ImmediateEntry(
      entry, [entry = this->entry](InflightT<ActionT> *) {
        // if reply failed, kernel won't hold a reference for this lookup.
        auto generation_to_clear = decrement_lookup_count(entry.ino, 1);
        if (generation_to_clear.has_value()) {
          best_effort_clear_inode_use_record(entry.ino, *generation_to_clear);
        }
      });
}
