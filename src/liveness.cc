
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
#include <sys/random.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <time.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "inflight.h"
#include "liveness.h"
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

struct ObservedPidState {
  std::optional<ProcessTableEntry> entry;
  std::optional<uint64_t> max_counter_seen;
  std::optional<struct timespec> max_counter_increase_boottime;
  uint64_t seen_in_scan_epoch = 0;
  std::optional<struct timespec> missing_since_boottime;
  bool malformed_entry = false;
};

std::mutex observed_pid_table_mutex;
std::map<std::vector<uint8_t>, ObservedPidState> observed_pid_table;
std::map<std::vector<uint8_t>, struct timespec> bogon_pids;
uint64_t observed_pid_scan_epoch = 0;
std::optional<struct timespec> observed_pid_last_scan_complete_boottime;
std::atomic<uint64_t> observed_pid_consecutive_scan_failures = 0;
constexpr time_t bogon_dead_after_sec = 5 * 60;
constexpr time_t complete_scan_fresh_sec = 10;

struct RawPidRecord {
  std::vector<uint8_t> key;
  std::vector<uint8_t> value;
};
struct PidScanResult {
  std::vector<RawPidRecord> records;
  bool complete = false;
  struct timespec scan_boottime = {};
};

static PidScanResult scan_pid_table() {
  const auto start = pack_pid_key({});
  auto stop = start;
  stop.push_back('\xff');

  PidScanResult result;
  std::vector<uint8_t> cursor = start;
  bool cursor_exclusive = false;

  unique_transaction t = make_transaction();
  clock_gettime(CLOCK_BOOTTIME, &result.scan_boottime);

  while (true) {
    const FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;

    unique_future f;
    if (cursor_exclusive) {
      f = wrap_future(fdb_transaction_get_range(
          t.get(), FDB_KEYSEL_FIRST_GREATER_THAN(cursor.data(), cursor.size()),
          FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(stop.data(), stop.size()), 0, 0,
          FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0));
    } else {
      f = wrap_future(fdb_transaction_get_range(
          t.get(), FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(cursor.data(), cursor.size()),
          FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(stop.data(), stop.size()), 0, 0,
          FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0));
    }
    fdb_error_t err = fdb_future_block_until_ready(f.get());
    if (err != 0) {
      return result;
    }
    err = fdb_future_get_keyvalue_array(f.get(), &kvs, &kvcount, &more);
    if (err != 0) {
      return result;
    }

    for (int i = 0; i < kvcount; i++) {
      const FDBKeyValue &kv = kvs[i];
      result.records.push_back(RawPidRecord{
          std::vector<uint8_t>(kv.key, kv.key + kv.key_length),
          std::vector<uint8_t>(kv.value, kv.value + kv.value_length)});
    }

    if (!more) {
      // only condition where we've finished cleanly, so mark the
      // scan as complete.
      result.complete = true;
      return result;
    }

    if (kvcount > 0) {
      const FDBKeyValue &kv = kvs[kvcount - 1];
      cursor.assign(kv.key, kv.key + kv.key_length);
      cursor_exclusive = true;
    } else {
      // weird edge case, no keys but the db says there's more.
      // there's nothing we can do with that, so we'll just assume
      // the scan failed and return our partial results.
      return result;
    }
  }
}

static void merge_pid_table_scan(const std::vector<RawPidRecord> &records,
                                 bool complete_scan,
                                 const struct timespec &scan_boottime) {
  const size_t pid_prefix_length = pack_pid_key({}).size();

  // this lock is (part of) why we separate this function from the scan
  // we don't want to hold it while we're doing an extended scan of the
  // database
  std::lock_guard<std::mutex> guard(observed_pid_table_mutex);
  observed_pid_scan_epoch += 1;
  const uint64_t scan_epoch = observed_pid_scan_epoch;

  for (const auto &record : records) {
    if (record.key.size() < pid_prefix_length) {
      // malformed key in pid space, ignore it.
      continue;
    }
    std::vector<uint8_t> observed_pid(record.key.begin() + pid_prefix_length,
                                      record.key.end());
    if (observed_pid == pid) {
      // we only cache observations for other processes.
      continue;
    }
    bogon_pids.erase(observed_pid);
    auto &state = observed_pid_table[observed_pid];
    state.seen_in_scan_epoch = scan_epoch;
    state.missing_since_boottime.reset();

    // check that:
    // - the entry parses
    // - the pid in it matches the pid in the key
    //   - size first, then contents
    ProcessTableEntry entry;
    if (!entry.ParseFromArray(record.value.data(), record.value.size()) ||
        !entry.IsInitialized() || (entry.pid().size() != observed_pid.size()) ||
        (memcmp(entry.pid().data(), observed_pid.data(), observed_pid.size()) !=
         0)) {
      state.malformed_entry = true;
      state.entry.reset();
      continue;
    }

    state.malformed_entry = false;
    state.entry = entry;
    if (!state.max_counter_seen.has_value() ||
        (entry.liveness_counter() > *state.max_counter_seen)) {
      state.max_counter_seen = entry.liveness_counter();
      state.max_counter_increase_boottime = scan_boottime;
    }
  }

  if (complete_scan) {
    // we finished a complete scan, so we can safely make adjustments
    // to any of the entries in the cache that we didn't see this time
    for (auto &[observed_pid, state] : observed_pid_table) {
      if (state.seen_in_scan_epoch == scan_epoch) {
        continue;
      }
      if (!state.missing_since_boottime.has_value()) {
        state.missing_since_boottime = scan_boottime;
      }
    }
    observed_pid_consecutive_scan_failures.store(0, std::memory_order_relaxed);
    observed_pid_last_scan_complete_boottime = scan_boottime;
  }
}

static void refresh_observed_pid_table() {
  const auto result = scan_pid_table();
  if ((result.records.size() > 0) || result.complete) {
    merge_pid_table_scan(result.records, result.complete, result.scan_boottime);
  }
  if (!result.complete) {
    observed_pid_consecutive_scan_failures.fetch_add(1,
                                                     std::memory_order_relaxed);
  }
}

PidLiveness classify_pid(const std::vector<uint8_t> &candidate_pid) {
  if (candidate_pid == pid) {
    return PidLiveness::Alive;
  }

  struct timespec now = {};
  clock_gettime(CLOCK_BOOTTIME, &now);

  auto elapsed_seconds = [](const struct timespec &start,
                            const struct timespec &end) -> uint64_t {
    time_t sec = end.tv_sec - start.tv_sec;
    long nsec = end.tv_nsec - start.tv_nsec;
    if (nsec < 0) {
      sec -= 1;
    }
    if (sec <= 0) {
      return 0;
    }
    return static_cast<uint64_t>(sec);
  };

  std::optional<struct timespec> last_complete_scan;
  bool observed_found = false;
  bool observed_has_valid_entry = false;
  std::optional<struct timespec> observed_missing_since;
  std::optional<struct timespec> bogon_since;

  {
    // hold the lock only briefly
    std::lock_guard<std::mutex> guard(observed_pid_table_mutex);
    last_complete_scan = observed_pid_last_scan_complete_boottime;

    auto it = observed_pid_table.find(candidate_pid);
    if (it != observed_pid_table.end()) {
      observed_found = true;
      const auto &state = it->second;
      observed_has_valid_entry =
          state.entry.has_value() && !state.malformed_entry;
      observed_missing_since = state.missing_since_boottime;
    } else {
      auto bogon_it = bogon_pids.find(candidate_pid);
      if (bogon_it == bogon_pids.end()) {
        bogon_pids[candidate_pid] = now;
        return PidLiveness::Unknown;
      }
      bogon_since = bogon_it->second;
    }
  }

  const bool complete_scan_recent =
      last_complete_scan.has_value() &&
      (elapsed_seconds(*last_complete_scan, now) <=
       static_cast<uint64_t>(complete_scan_fresh_sec));
  const bool complete_scan_healthy =
      complete_scan_recent && (observed_pid_consecutive_scan_failures.load(
                                   std::memory_order_relaxed) == 0);

  if (observed_found) {
    if (!observed_has_valid_entry) {
      return PidLiveness::Unknown;
    }
    if (!observed_missing_since.has_value()) {
      return PidLiveness::Alive;
    }

    const uint64_t missing_for = elapsed_seconds(*observed_missing_since, now);
    if (complete_scan_healthy &&
        (missing_for >= static_cast<uint64_t>(bogon_dead_after_sec))) {
      return PidLiveness::Dead;
    }
    return PidLiveness::Unknown;
  }

  if (!bogon_since.has_value()) {
    return PidLiveness::Unknown;
  }

  const uint64_t bogon_for = elapsed_seconds(*bogon_since, now);
  if (complete_scan_healthy &&
      (bogon_for >= static_cast<uint64_t>(bogon_dead_after_sec))) {
    return PidLiveness::Dead;
  }
  return PidLiveness::Unknown;
}

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

    if (!fdb_set_protobuf(t, key, pt_entry)) {
      // failure shouldn't be possible since we control the inputs here
      std::terminate();
    }

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
  refresh_observed_pid_table();

  while (!terminate) {
    struct timespec sleep;
    sleep.tv_sec = liveness_refresh_sec;
    sleep.tv_nsec = liveness_refresh_nsec;
    nanosleep(&sleep, NULL);

    update_pt_entry_time();
    pt_entry.set_liveness_counter(pt_entry.liveness_counter() + 1);

    send_pt_entry(false);
    refresh_observed_pid_table();
  }

  return NULL;
}

// returns false if liveness is started by the time the function returns
bool start_liveness(struct fuse_session *se) {
  if (liveness_started) {
    return false;
  }

  pid.clear();

  // fill a 128-bit pid from the kernel RNG.
  pid.resize(16);
  size_t bytes_read = 0;
  while (bytes_read < pid.size()) {
    const ssize_t n =
        getrandom(pid.data() + bytes_read, pid.size() - bytes_read, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      pid.clear();
      return true;
    }
    bytes_read += static_cast<size_t>(n);
  }

  // we make main pass this in so it isn't floating around
  // in every namespace.
  fuse_session = se;

  terminate = false;

  if (pthread_create(&liveness_thread, NULL, liveness_manager, NULL)) {
    terminate = true;
    fuse_session = NULL;
    pid.clear();
    return true;
  }

  liveness_started = true;
  return false;
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
    if (maybe_jt.has_value())
      it = *maybe_jt;
    else
      /* if there was failure, don't advance it; we'll try again. */;
  }

  pid.clear();
  pt_entry.Clear();
  fuse_session = NULL;
}
