
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <time.h>

#include <memory>
#include <mutex>
#include <string>

#include "inflight.h"
#include "util.h"
#include "values.pb.h"

/*************************************************************
 * liveness management
 *************************************************************
 * initially for garbage collection purposes, filesystems need to be
 * able to agree which processes are still running, and clean up
 * records of dead ones.
 */

std::vector<uint8_t> pid;
ProcessTableEntry pt_entry;
struct fuse_session *fuse_session;
pthread_t liveness_thread;
bool terminate = true;
bool liveness_started = false;

void send_pt_entry(bool startup) {
  std::function<int(FDBTransaction *)> f = [startup](FDBTransaction *t) {
    const auto key = pack_pid_key(pid);
    fdb_error_t err;
    if (!startup) {
      unique_future g;
      g.reset(fdb_transaction_get(t, key.data(), key.size(), 0));
      err = fdb_future_block_until_ready(g.get());
      if (err != 0) {
        throw err;
      }
      fdb_bool_t present;
      const uint8_t *value;
      int value_length;
      err = fdb_future_get_value(g.get(), &present, &value, &value_length);
      if (err != 0) {
        throw err;
      }

      if (!present) {
        // our entry in the table was removed.
        terminate = true;

        // kill everything in-flight
        shut_it_down();

        // stop processing filesystem calls immediately.
        // the fuse loop won't notice this until the next
        // filesystem operation comes in. sigh.
        fuse_session_exit(fuse_session);

        return 0;
      }
    }
    const int entry_size = pt_entry.ByteSizeLong();
    uint8_t entry_buffer[entry_size];
    pt_entry.SerializeToArray(entry_buffer, entry_size);

    fdb_transaction_set(t, key.data(), key.size(), entry_buffer, entry_size);
    return 0;
  };
  run_sync_transaction(f);
}

const int liveness_refresh_sec = 0;
const int liveness_refresh_nsec = 500 * 1000000;

void update_pt_entry_time() {
  struct timespec tv;
  clock_gettime(CLOCK_REALTIME, &tv);

  pt_entry.mutable_last_updated()->set_sec(tv.tv_sec);
  pt_entry.mutable_last_updated()->set_nsec(tv.tv_nsec);
}

std::mutex manager_running;
void *liveness_manager(void *ignore) {
  const std::unique_lock<std::mutex> lock(manager_running);

  pt_entry.set_pid(pid.data(), pid.size());
  pt_entry.set_liveness_counter(0);
  update_pt_entry_time();
  struct utsname buf;
  uname(&buf);
  pt_entry.set_hostname(buf.nodename);

  send_pt_entry(true);

  while (!terminate) {
    struct timespec sleep;
    sleep.tv_sec = liveness_refresh_sec;
    sleep.tv_nsec = liveness_refresh_nsec;
    nanosleep(&sleep, NULL);

    update_pt_entry_time();
    pt_entry.set_liveness_counter(pt_entry.liveness_counter() + 1);

    send_pt_entry(false);
  }

  return NULL;
}

void start_liveness(struct fuse_session *se) {
  if (liveness_started) {
    return;
  }

  pid.clear();

  // this probably isn't the best way to produce 128 bits in
  // a std::vector, but, whatever.
  for (int i = 0; i < 16; i++) {
    pid.push_back(random() & 0xFF);
  }

  // we make main pass this in so it isn't floating around
  // in every namespace.
  fuse_session = se;

  terminate = false;

  if (pthread_create(&liveness_thread, NULL, liveness_manager, NULL)) {
    terminate = true;
    fuse_session = NULL;
    pid.clear();
    return;
  }

  liveness_started = true;
}

// we're being called after unmount
void terminate_liveness() {
  if (!liveness_started || pid.empty()) {
    return;
  }
  liveness_started = false;

  terminate = true;

  // wait until the liveness_manager is done
  const std::unique_lock<std::mutex> lock(manager_running);

  // clear our PID record
  std::function<int(FDBTransaction *)> f = [](FDBTransaction *t) {
    const auto start = pack_pid_key(pid);
    auto stop = start;
    stop.push_back('\xff');
    fdb_transaction_clear_range(t, start.data(), start.size(), stop.data(),
                                stop.size());
    return 0;
  };
  run_sync_transaction(f);

  // TODO are all of our outstanding transactions dead?

  // TODO this is rather coarse locking, it might be possible to tighten
  // it up, but it's not clear to me that it's necessary. ideally we would
  // wait for all inflight transactions to finish before we reach this point.
  std::lock_guard<std::mutex> guard(lookup_counts_mutex);
  int clears_per_batch = 64;
  for (auto it = lookup_counts.cbegin(); it != lookup_counts.cend();) {
    // it is at this moment that i begin to question my use of c++
    std::function<decltype(lookup_counts.cbegin())(FDBTransaction *)> f =
        [it, clears_per_batch](FDBTransaction *t) {
          auto jt = it;
          for (int count = 0;
               (jt != lookup_counts.cend()) && (count < clears_per_batch);
               jt++, count++) {
            auto key = pack_inode_use_key(jt->first);
            fdb_transaction_clear(t, key.data(), key.size());
          }
          return jt;
        };
    auto maybe_jt = run_sync_transaction(f);
    if (maybe_jt)
      it = *maybe_jt;
    else
      /* if there was failure, don't advance it; we'll try again. */;
  }

  pid.clear();
  pt_entry.Clear();
  fuse_session = NULL;
}
