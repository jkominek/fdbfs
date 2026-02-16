
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>

#include "inflight.h"
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

static void fail_inflight(Inflight *i, int err, const char *why) {
  // TODO log or print out 'why'
  if (!i->suppress_errors)
    fuse_reply_err(i->req, err);
  i->cleanup();
  delete i;
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
  // we need to be more clever about this. having every single
  // operation fetch a read version is going to add a lot of latency.
#if DEBUG
  clock_gettime(CLOCK_MONOTONIC, &clockstart);
#endif
}

AttemptState &Inflight::attempt_state() { return *attempt; }

const AttemptState &Inflight::attempt_state() const { return *attempt; }

void Inflight::reset_attempt_state() { attempt = create_attempt_state(); }

void Inflight::cleanup() {
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

  attempt_state().cb.emplace(issue());
  if (attempt_state().future_queue.empty()) {
    run_current_callback();
    return;
  }
  begin_wait();
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
    debug_print("fdbfs_error_processor killing request %p for inflight %p: %s",
                inflight->req, p, fdb_get_error(err));
    // error during an error. foundationdb says that means
    // you should give up. so we'll let fuse know they're hosed.
    if (!inflight->suppress_errors)
      fuse_reply_err(inflight->req, EIO);

    delete inflight;
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
    // got an error during normal processing. foundationdb says
    // we should call _on_error on it, and maybe we'll get to
    // try again, and maybe we won't.
    FDBFuture *nextf =
        fdb_transaction_on_error(inflight->transaction.get(), err);

    inflight->reset_attempt_state();
    if (fdb_future_set_callback(nextf, fdbfs_error_processor,
                                static_cast<void *>(inflight))) {
      // hosed, we don't currently have a way to save this transaction.
      // TODO let fuse know the operation is dead.
      fail_inflight(inflight, EIO, "fdb_future_set_callback returned an error");
    }
    return;
  }

  // and finally toss the next bit of work onto the thread pool queue
  pool.push_task([inflight, f]() { inflight->future_ready(f); });
}

// Inflight_markused

Inflight_markused::Inflight_markused(fuse_req_t req, uint64_t generation,
                                     struct fuse_entry_param entry,
                                     unique_transaction transaction)
    : InflightWithAttempt(req, ReadWrite::Yes, std::move(transaction)),
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
