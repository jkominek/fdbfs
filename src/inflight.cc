
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <atomic>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <stdexcept>

#include "inflight.h"
#include "liveness.h"
#include "util.h"

thread_pool pool;

bool shut_it_down_forever = false;

void shut_it_down() {
  // all the future handlers will ENOSYS when they're
  // invoked. we could do this a little bit faster if
  // we kept a set (or something) of all of the inflights,
  // so we could just go down the list and delete them all.
  shut_it_down_forever = true;
}

static void
fail_inflight(Inflight *i, int err, const char *why,
              std::source_location loc = std::source_location::current()) {
  if (err != 0) {
    InflightAction::trace_errno_error("FailInflight", err, why, loc);
  }
  if (!i->suppress_errors)
    fuse_reply_err(i->req, err);
  i->cleanup();
  delete i;
}

namespace {

constexpr uint8_t OPLOG_PREFIX = 'o';

std::mutex oplog_tracking_mutex;
std::set<uint64_t> oplog_live_ids;
std::set<uint64_t> oplog_dead_ids;
std::atomic<uint64_t> next_oplog_id = 1;

uint64_t allocate_oplog_id() {
  const uint64_t id = next_oplog_id.fetch_add(1, std::memory_order_relaxed);
  if (id == 0) {
    // wrapped around; would allow reuse of old op ids.
    // TODO allow wrap-around.
    std::terminate();
  }
  return id;
}

void mark_oplog_live(uint64_t op_id) {
  std::lock_guard<std::mutex> guard(oplog_tracking_mutex);
  oplog_live_ids.insert(op_id);
}

void mark_oplog_dead(uint64_t op_id) {
  std::lock_guard<std::mutex> guard(oplog_tracking_mutex);
  oplog_live_ids.erase(op_id);
  oplog_dead_ids.insert(op_id);
}

std::vector<uint8_t> pack_oplog_prefix(const std::vector<uint8_t> &owner_pid) {
  std::vector<uint8_t> key = key_prefix;
  key.push_back(OPLOG_PREFIX);
  key.insert(key.end(), owner_pid.begin(), owner_pid.end());
  return key;
}

// requires a scratch buffer from the caller
const char *op_id_to_cstr(const std::optional<uint64_t> &op_id, char *buf,
                          size_t buflen) {
  if (!op_id.has_value()) {
    return "null";
  }
  snprintf(buf, buflen, "%llu", static_cast<unsigned long long>(*op_id));
  return buf;
}

} // namespace

std::vector<uint8_t> pack_oplog_key(const std::vector<uint8_t> &owner_pid,
                                    uint64_t op_id) {
  std::vector<uint8_t> key = pack_oplog_prefix(owner_pid);
  const uint64_t op_id_be = htobe64(op_id);
  const auto *tmp = reinterpret_cast<const uint8_t *>(&op_id_be);
  key.insert(key.end(), tmp, tmp + sizeof(op_id_be));
  return key;
}

range_keys pack_oplog_subspace_range(const std::vector<uint8_t> &owner_pid) {
  auto start = pack_oplog_prefix(owner_pid);
  auto stop = prefix_range_end(start);
  return {start, stop};
}

range_keys pack_local_oplog_span_range(uint64_t start_op_id,
                                       uint64_t stop_op_id) {
  assert(start_op_id < stop_op_id);
  auto start = pack_oplog_key(pid, start_op_id);
  auto stop = pack_oplog_key(pid, stop_op_id);
  assert(start < stop);
  return {start, stop};
}

// cleans up our in-memory oplog records, and returns the
// oplog key range that can be cleared. if the records aren't
// cleared successfully, they'll leak. (and cause problems if
// we implement wrap-around)
std::optional<std::pair<uint64_t, uint64_t>> claim_local_oplog_cleanup_span() {
  std::lock_guard<std::mutex> guard(oplog_tracking_mutex);
  if (oplog_dead_ids.empty()) {
    return std::nullopt;
  }

  const uint64_t min_dead = *oplog_dead_ids.begin();
  uint64_t stop = 0;
  std::set<uint64_t>::iterator erase_end;

  if (oplog_live_ids.empty()) {
    const uint64_t max_dead = *oplog_dead_ids.rbegin();
    if (max_dead == std::numeric_limits<uint64_t>::max()) {
      // we reserve overflow as impossible by design.
      std::terminate();
    }
    stop = max_dead + 1;
    erase_end = oplog_dead_ids.end();
  } else {
    const uint64_t min_live = *oplog_live_ids.begin();
    if (min_dead >= min_live) {
      // NOTE if this situation persists, we'll fail to
      // make progress clearing out the op log
      return std::nullopt;
    }
    erase_end = oplog_dead_ids.lower_bound(min_live);
    if (erase_end == oplog_dead_ids.begin()) {
      return std::nullopt;
    }
    const uint64_t max_clear = *std::prev(erase_end);
    if (max_clear == std::numeric_limits<uint64_t>::max()) {
      std::terminate();
    }
    stop = max_clear + 1;
  }

  oplog_dead_ids.erase(oplog_dead_ids.begin(), erase_end);
  return std::make_pair(min_dead, stop);
}

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
// readwrite specifies the transaction will include writes
Inflight::Inflight(fuse_req_t req, ReadWrite readwrite,
                   unique_transaction provided)
    : transaction(std::move(provided)), req(req), readwrite(readwrite) {
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

AttemptState &Inflight::attempt_state() { return *attempt; }

const AttemptState &Inflight::attempt_state() const { return *attempt; }

void Inflight::reset_attempt_state() { attempt = create_attempt_state(); }

InflightAction Inflight::oplog_recovery(const OpLogRecord &) {
  // it shouldn't be possible to reach this code, anything that
  // declares itself as ReadWrite::Yes must override this.
  // TODO enforce in the typesystem, by having separate Inflight
  // subclasses for each ReadWrite value? (and then eliminating
  // the value)
  return InflightAction::Abort(EIO);
}

bool Inflight::write_oplog_record(OpLogRecord record) {
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

bool Inflight::write_oplog_result(const OpLogResultVariant &result) {
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

void Inflight::cleanup() {
  if (op_id.has_value()) {
    mark_oplog_dead(*op_id);
  }
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
}

bool Inflight::should_check_oplog() const {
  return uses_oplog() && commit_unknown_seen;
}

bool Inflight::uses_oplog() const { return readwrite == ReadWrite::Yes; }

InflightAction Inflight::check_oplog_or_issue() {
  fdb_bool_t present = 0;
  const uint8_t *val = nullptr;
  int vallen = 0;
  const fdb_error_t err = fdb_future_get_value(
      attempt_state().oplog_fetch.get(), &present, &val, &vallen);
  if (err) {
    return InflightAction::FDBError(err);
  }

  if (!present) {
    // no prior committed result found, continue with normal retry path.
    InflightCallback next_cb = issue();
    if (attempt_state().future_queue.empty()) {
      return next_cb();
    }
    return InflightAction::BeginWait(next_cb);
  }

  OpLogRecord record;
  if (!record.ParseFromArray(val, vallen) || !record.IsInitialized()) {
    return InflightAction::Abort(EIO);
  }
  if (!op_id.has_value() || record.op_id() != *op_id) {
    return InflightAction::Abort(EIO);
  }
  if (record.result_case() == OpLogRecord::RESULT_NOT_SET) {
    return InflightAction::Abort(EIO);
  }

  return oplog_recovery(record);
}

void Inflight::start() {
  if (!attempt) {
    reset_attempt_state();
  }

  if (shut_it_down_forever) {
    // abort everything immediately
    fuse_reply_err(req, ENOSYS);
    cleanup();
    delete this;
    return;
  }

  if (const fdb_error_t err = configure_transaction()) {
    if (fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, err)) {
      (void)retry_with_on_error(err, "configure_transaction");
    } else {
      InflightAction::trace_fdb_error("ConfigureTransaction", err,
                                      "configure_transaction failed",
                                      std::source_location::current());
      fail_inflight(this, EIO, "configure_transaction failed");
    }
    return;
  }

  if (should_check_oplog()) {
    if (!op_id.has_value()) {
      // this should be impossible
      fail_inflight(this, EIO, "oplog check requested without op_id");
      return;
    }
    const auto key = pack_oplog_key(pid, *op_id);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        attempt_state().oplog_fetch);
    attempt_state().cb.emplace(
        std::bind(&Inflight::check_oplog_or_issue, this));
  } else {
    attempt_state().cb.emplace(issue());
  }

  if (attempt_state().future_queue.empty()) {
    run_current_callback();
    return;
  }
  begin_wait();
}

bool Inflight::retry_with_on_error(fdb_error_t err, const char *source) {
  // we're here because some fdb function in an Inflight subclass
  // returned an fdb_error_t, then FDB said it was a retryable error, so now
  // we've got to run on_error to either 1) wait (fdb controlled backoff) or 2)
  // perhaps permanently fail
  if (InflightAction::trace_errors_enabled()) {
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
    fail_inflight(this, EIO, "fdb_transaction_on_error returned nullptr");
    return false;
  }

  reset_attempt_state();
  if (fdb_future_set_callback(nextf, fdbfs_error_processor,
                              static_cast<void *>(this))) {
    // Avoid leaking the on_error future when callback setup fails.
    fdb_future_destroy(nextf);
    fail_inflight(this, EIO, "fdb_future_set_callback returned an error");
    return false;
  }

  return true;
}

InflightAction Inflight::commit(InflightCallback cb) {
  wait_on_future(fdb_transaction_commit(transaction.get()),
                 attempt_state().commit);
  return InflightAction::BeginWait(cb);
}

bool Inflight::run_current_callback() {
  if (bool(attempt_state().cb)) {
    std::function<InflightAction()> f = attempt_state().cb.value();
    attempt_state().cb = std::nullopt;
    InflightAction a = f();
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
  fail_inflight(this, EIO, "no callback was set when we needed one");
  return false;
}

void Inflight::future_ready(FDBFuture *f) {
  assert(!attempt_state().future_queue.empty());
  if (attempt_state().future_queue.empty()) {
    fail_inflight(this, EIO, "future_ready called with empty queue");
    return;
  }

  // only the first future should call us
  FDBFuture *next = attempt_state().future_queue.front();
  attempt_state().future_queue.pop();
  assert(next == f);
  if (next != f) {
    fail_inflight(this, EIO, "future_ready called for unexpected future");
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

void Inflight::begin_wait() {
  if (attempt_state().future_queue.empty()) {
    // we could try to resume processing the transaction, but really this
    // just shouldn't be possible, so i'm inclined to bail on it.
    fail_inflight(this, EIO, "tried to start waiting on empty future queue");
    return;
  }
  if (fdb_future_set_callback(attempt_state().future_queue.front(),
                              fdbfs_error_checker, static_cast<void *>(this))) {
    // we don't have a way to deal with this, so destroy the
    // transaction so that nothing leaks. ideally we'd notify
    // fuse at this point that the operation is dead.
    fail_inflight(this, EIO, "failed to set future callback");
    return;
  }
}

void Inflight::wait_on_future(FDBFuture *f, unique_future &dest) {
  attempt_state().future_queue.push(f);
  dest.reset(f);
}

/* If fdbfs_error_checker hits an error, then execution will
 * end up here for that transaction. We give it back to FDB
 * so that it can delay this transaction, or whatever it wants
 * to do, as appropriate.
 */
extern "C" void fdbfs_error_processor(FDBFuture *f, void *p) {
  Inflight *inflight = static_cast<Inflight *>(p);

  if (shut_it_down_forever) {
    // everything is to be terminated immediately
    fuse_reply_err(inflight->req, ENOSYS);
    delete inflight;
    return;
  }

  fdb_error_t err = fdb_future_get_error(f);
  // done with this either way.
  fdb_future_destroy(f);

  if (err) {
    if (InflightAction::trace_errors_enabled()) {
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
    fail_inflight(inflight, EIO, "fdb_transaction_on_error future failed");
    return;
  }

  // foundationdb, perhaps after some delay, has given us the
  // goahead to start up the new transaction.
  fdb_transaction_reset(inflight->transaction.get());
  inflight->start();
}

/*
 * This is the entry point for all FDB callbacks.  So we can
 * centralize our error checking, restart transactions in the event of
 * errors, and enqueue the next step of the transaction into the thread
 * pool for execution.
 */
extern "C" void fdbfs_error_checker(FDBFuture *f, void *p) {
  Inflight *inflight = static_cast<Inflight *>(p);

  fdb_error_t err = fdb_future_get_error(f);

  if (err) {
    if (InflightAction::trace_errors_enabled()) {
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

// Inflight_markused

Inflight_markused::Inflight_markused(fuse_req_t req, uint64_t generation,
                                     struct fuse_entry_param entry,
                                     unique_transaction transaction)
    : InflightWithAttempt(req, ReadWrite::IdempotentWrite,
                          std::move(transaction)),
      generation(generation), entry(entry) {}

InflightCallback Inflight_markused::issue() {
  auto inode_key = pack_inode_key(entry.ino);
  wait_on_future(fdb_transaction_get(transaction.get(), inode_key.data(),
                                     inode_key.size(), 0),
                 a().inode_fetch);
  return std::bind(&Inflight_markused::check_inode, this);
}

InflightAction Inflight_markused::check_inode() {
  fdb_bool_t present = 0;
  const uint8_t *val = nullptr;
  int vallen = 0;
  fdb_error_t err =
      fdb_future_get_value(a().inode_fetch.get(), &present, &val, &vallen);
  if (err)
    return InflightAction::FDBError(err);

  if (!present) {
    (void)decrement_lookup_count(entry.ino, 1);
    return InflightAction::Abort(EIO);
  }

  auto use_key = pack_inode_use_key(entry.ino);
  const uint64_t generation_le = htole64(generation);
  fdb_transaction_atomic_op(transaction.get(), use_key.data(), use_key.size(),
                            reinterpret_cast<const uint8_t *>(&generation_le),
                            sizeof(generation_le), FDB_MUTATION_TYPE_MAX);
  return commit(std::bind(&Inflight_markused::reply_entry, this));
}

InflightAction Inflight_markused::reply_entry() {
  if (fuse_reply_entry(req, &entry) < 0) {
    // if reply failed, kernel won't hold a reference for this lookup.
    auto generation_to_clear = decrement_lookup_count(entry.ino, 1);
    if (generation_to_clear.has_value()) {
      best_effort_clear_inode_use_record(entry.ino, *generation_to_clear);
    }
  }
  return InflightAction::Ignore();
}
