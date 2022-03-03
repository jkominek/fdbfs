
#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>

#include <iostream>
#include <memory>
#include <stdexcept>
#include <functional>

#include "util.h"
#include "inflight.h"

bool shut_it_down_forever = false;

void shut_it_down() {
  // all the future handlers will ENOSYS when they're
  // invoked. we could do this a little bit faster if
  // we kept a set (or something) of all of the inflights,
  // so we could just go down the list and delete them all.
  shut_it_down_forever = true;
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
Inflight::Inflight(fuse_req_t req, ReadWrite readwrite, unique_transaction provided)
  : transaction(std::move(provided)), req(req), readwrite(readwrite)
{
  // we need to be more clever about this. having every single
  // operation fetch a read version is going to add a lot of latency.
#if DEBUG
  clock_gettime(CLOCK_MONOTONIC, &clockstart);
#endif
}

void Inflight::cleanup()
{
#if DEBUG
  struct timespec stop;
  clock_gettime(CLOCK_MONOTONIC, &stop);
  time_t secs = (stop.tv_sec - clockstart.tv_sec);
  long nsecs = (stop.tv_nsec - clockstart.tv_nsec);
  if(secs<5) {
    nsecs += secs * 1000000000;
    printf("inflight %p for req %p took %li ns\n", this, req, nsecs);
  } else {
    printf("inflight %p for req %p took %li s\n", this, req, secs);
  }
#endif
}

void Inflight::start()
{
  if(shut_it_down_forever) {
    // abort everything immediately
    fuse_reply_err(req, ENOSYS);
    cleanup();
    delete this;
    return;
  }

  cb.emplace(issue());
  begin_wait();
}

void Inflight::future_ready(FDBFuture *f)
{
  if(!future_queue.empty()) {
    // only the first future should call us
    FDBFuture *next = future_queue.front();
    future_queue.pop();
    if(next != f) {
      // TODO error? or something? what?
    }
    // skip over any futures that are already ready
    while((!future_queue.empty()) && fdb_future_is_ready(future_queue.front()))
      future_queue.pop();
  } else {
    // hmm. wtf?
  }

  if(future_queue.empty()) {
    if(bool(cb)) {
      std::function<InflightAction()> f = cb.value();
      cb = std::nullopt;
      InflightAction a = f();
      a.perform(this);

      if(a.begin_wait) {
	begin_wait();
      }

      if(a.restart) {
	fdb_transaction_reset(transaction.get());
	this->reincarnate()->start();
      }
      
      if(a.delete_this) {
	// we're dead and done, for whatever reason
        cleanup();
	delete this;
      }
    } else {
      std::cout << "no callback was set for " << typeid(*this).name() << " when we needed one" << std::endl;
      throw std::runtime_error("no callback was set when we needed one");
    }
  } else {
    begin_wait();
  }
}

void Inflight::begin_wait() {
  if(future_queue.empty()) {
    std::cout << "tried to start waiting on empty future queue" << std::endl;
    throw std::runtime_error("tried to start waiting on empty future queue");
  }
  if(fdb_future_set_callback(future_queue.front(),
			     fdbfs_error_checker,
			     static_cast<void*>(this))) {
    std::cout << "failed to set future callback" << std::endl;
    throw std::runtime_error("failed to set future callback");
  }
}

void Inflight::wait_on_future(FDBFuture *f, unique_future *dest)
{
  future_queue.push(f);
  dest->reset(f);
}

extern "C" void fdbfs_error_processor(FDBFuture *f, void *p)
{
  Inflight *inflight = static_cast<Inflight*>(p);

  if(shut_it_down_forever) {
    // everything is to be terminated immediately
    fuse_reply_err(inflight->req, ENOSYS);
    delete inflight;
    return;
  }

  fdb_error_t err = fdb_future_get_error(f);
  // done with this either way.
  fdb_future_destroy(f);

  if(err) {
    debug_print("fdbfs_error_processor killing request %p for inflight %p: %s",
		inflight->req, p, fdb_get_error(err));
    // error during an error. foundationdb says that means
    // you should give up. so we'll let fuse know they're hosed.
    if(!inflight->suppress_errors)
      fuse_reply_err(inflight->req, EIO);

    delete inflight;
    return;
  }
    
  // foundationdb, perhaps after some delay, has given us the
  // goahead to start up the new transaction.
  fdb_transaction_reset(inflight->transaction.get());
  inflight->start();
}

extern "C" void fdbfs_error_checker(FDBFuture *f, void *p)
{
  Inflight *inflight = static_cast<Inflight*>(p);

  fdb_error_t err = fdb_future_get_error(f);

  if(err) {
    // got an error during normal processing. foundationdb says
    // we should call _on_error on it, and maybe we'll get to
    // try again, and maybe we won't.
    FDBFuture *nextf = fdb_transaction_on_error(inflight->transaction.get(), err);
    
    if(fdb_future_set_callback(nextf, fdbfs_error_processor,
			       static_cast<void*>(inflight->reincarnate()))) {
      throw std::runtime_error("failed to set an fdb callback");
    }
    return;
  }

  inflight->future_ready(f);
}


// Inflight_markused

Inflight_markused::Inflight_markused(fuse_req_t req, fuse_ino_t ino, unique_transaction transaction)
  : Inflight(req, ReadWrite::Yes, std::move(transaction)), ino(ino)
{
  // we're taking place after fuse has already received the
  // real response. it doesn't care what we have to say, now.
  suppress_errors = true;
}

Inflight_markused *Inflight_markused::reincarnate() {
  Inflight_markused *x = new Inflight_markused(req, ino, std::move(transaction));
  delete this;
  return x;
}

InflightCallback Inflight_markused::issue() {
  auto key = pack_inode_use_key(ino);
  uint8_t b = 0;
  // TODO we should check to make sure the inode still exists
  // as part of this transaction. if it doesn't exist, we should
  // call fuse_lowlevel_notify_inval_inode. *** that function can
  // block until other operations complete!!! *** so if we call it
  // from within an fdb callback handler, we're deadlocked. don't do
  // that.
  fdb_transaction_set(transaction.get(),
		      key.data(), key.size(),
		      &b, 1);
  wait_on_future(fdb_transaction_commit(transaction.get()),
		 &commit);
  return []() -> InflightAction {
    return InflightAction::Ignore();
  };
}
