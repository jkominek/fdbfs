

#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "inflight.h"
#include "liveness.h"
#include "util.h"

/*************************************************************
 * garbage collector
 *************************************************************
 * scan the garbage key space
 * TODO we need another thread to regularly refresh our local
 * copy of the PID table, and 'ping' any other processes which
 * appear to be dead. we should also regularly check that our
 * entry in the PID table hasn't been marked with a flag telling
 * us to die. and if we haven't been able to check that for
 * awhile, we should die.
 */

pthread_t gc_thread;
std::atomic<bool> gc_stop = false;

constexpr int gc_scan_batch_limit = 64;

static void process_gc_candidate(const std::vector<uint8_t> &garbage_key) {
  // check for malformed keys in garbage key space
  if (garbage_key.size() != static_cast<size_t>(inode_key_length)) {
    // yeah length is wrong so it can't be a garbage key
    unique_transaction t = make_transaction();
    fdb_transaction_clear(t.get(), garbage_key.data(), garbage_key.size());
    auto f = wrap_future(fdb_transaction_commit(t.get()));
    const fdb_error_t wait_err = fdb_future_block_until_ready(f.get());
    (void)wait_err;
    return;
  }

  // fetch the inode from the garbage key.
  fuse_ino_t ino;
  bcopy(garbage_key.data() + key_prefix.size() + 1, &ino, sizeof(fuse_ino_t));
  ino = be64toh(ino);
  if (lookup_count_nonzero(ino)) {
    // we're still using the inode, don't bother checking the use records.
    return;
  }

  unique_transaction t = make_transaction();

#if DEBUG
  printf("found garbage inode %lx\n", ino);
#endif

  auto [start, stop] = pack_inode_use_subspace_range(ino);

  bool scan_complete = false;
  bool scan_error = false;
  bool inode_in_use = false;
  std::vector<std::vector<uint8_t>> dead_use_keys;
  std::vector<uint8_t> cursor = start;
  bool cursor_exclusive = false;
  while (true) {
    const FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;

    // despite having the code to do a loop, the expected situation
    // is that there will be one or two use records, and they'll both
    // be for live hosts. so instead of WANT_ALL we use SMALL. if the
    // servers give us a partial result, that's fine, the first key
    // will probably tell us what we need to know.
    unique_future f;
    if (cursor_exclusive) {
      f = wrap_future(fdb_transaction_get_range(
          t.get(), FDB_KEYSEL_FIRST_GREATER_THAN(cursor.data(), cursor.size()),
          FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(stop.data(), stop.size()), 64, 0,
          FDB_STREAMING_MODE_SMALL, 0, 0, 0));
    } else {
      f = wrap_future(fdb_transaction_get_range(
          t.get(),
          FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(cursor.data(), cursor.size()),
          FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(stop.data(), stop.size()), 64, 0,
          FDB_STREAMING_MODE_SMALL, 0, 0, 0));
    }
    if (fdb_future_block_until_ready(f.get()) ||
        fdb_future_get_keyvalue_array(f.get(), &kvs, &kvcount, &more)) {
      scan_error = true;
      break;
    }

    for (int i = 0; i < kvcount; i++) {
      const FDBKeyValue &kv = kvs[i];

      if ((kv.key_length <= inode_key_length) ||
          (kv.key[inode_key_length] != INODE_USE_PREFIX)) {
        // malformed use record key in use range. be conservative.
        inode_in_use = true;
        continue;
      }

      const int pid_offset = inode_key_length + 1;
      if (kv.key_length <= pid_offset) {
        // missing pid suffix.
        inode_in_use = true;
        continue;
      }

      std::vector<uint8_t> key(kv.key, kv.key + kv.key_length);
      std::vector<uint8_t> use_pid(kv.key + pid_offset, kv.key + kv.key_length);
      switch (classify_pid(use_pid)) {
      case PidLiveness::Dead:
        dead_use_keys.push_back(std::move(key));
        break;
      case PidLiveness::Alive:
      case PidLiveness::Unknown:
      default:
        inode_in_use = true;
        break;
      }
    }

    if (!more) {
      scan_complete = true;
      break;
    }

    if (kvcount > 0) {
      const FDBKeyValue &kv = kvs[kvcount - 1];
      cursor.assign(kv.key, kv.key + kv.key_length);
      cursor_exclusive = true;
    } else {
      // partial scan with no keys returned; don't treat as complete.
      scan_complete = false;
      break;
    }
  }

  if (scan_error) {
    // don't risk erasing anything if there has been an error
    return;
  }

  for (const auto &key : dead_use_keys) {
    fdb_transaction_clear(t.get(), key.data(), key.size());
  }

  // it shouldn't be possible for this to be true, but if it is, that's
  // a problem.
  if (lookup_count_nonzero(ino)) {
    // we've started using the inode; do not erase it.
    inode_in_use = true;
  }

  if (!scan_complete || inode_in_use) {
    // still in use (or uncertain), but clear any dead-host use records.
    if (dead_use_keys.empty()) {
      return;
    }
    auto f = wrap_future(fdb_transaction_commit(t.get()));
    const fdb_error_t wait_err = fdb_future_block_until_ready(f.get());
    (void)wait_err;
    return;
  }

  // no usage and scan is complete, erase it.
#if DEBUG
  printf("cleaning garbage inode\n");
#endif
  fdb_transaction_clear(t.get(), garbage_key.data(), garbage_key.size());
  // NOTE we could be paranoid and fetch the inode and doublecheck that
  // nlinks==0. but what if nlinks>0 where's the dirent? almost need to
  // add it to /lost+found or something
  erase_inode(t.get(), ino);

  auto f = wrap_future(fdb_transaction_commit(t.get()));
  // on failure, we can only proceed and try again in a future scan.
  const fdb_error_t wait_err = fdb_future_block_until_ready(f.get());
  (void)wait_err;
}

void *garbage_scanner(void *ignore) {
  struct timespec ts;
  auto [gc_start_key, gc_stop_key] = pack_garbage_subspace_range();
  std::vector<uint8_t> last_key = gc_start_key;
#if DEBUG
  printf("gc starting\n");
#endif
  while (!gc_stop.load(std::memory_order_relaxed)) {
    // TODO vary this or do something to slow things down.
    ts.tv_sec = 1;
    ts.tv_nsec = 0;
    nanosleep(&ts, NULL);

    if (auto span = claim_local_oplog_cleanup_span(); span.has_value()) {
      const auto range = pack_local_oplog_span_range(span->first, span->second);
      run_sync_transaction<int>([&](FDBTransaction *t) {
        fdbfs_transaction_clear_range(t, range);
        return 0;
      });
    }

    unique_transaction scan_t = make_transaction();
    unique_future f = wrap_future(fdb_transaction_get_range(
        scan_t.get(),
        FDB_KEYSEL_FIRST_GREATER_THAN(last_key.data(), last_key.size()),
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(gc_stop_key.data(),
                                          gc_stop_key.size()),
        gc_scan_batch_limit, 0, FDB_STREAMING_MODE_LARGE, 0, 0, 0));
    const FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;
    if (fdb_future_block_until_ready(f.get()) ||
        fdb_future_get_keyvalue_array(f.get(), &kvs, &kvcount, &more)) {
      // error, take a longer break.
      ts.tv_sec = 3;
      nanosleep(&ts, NULL);
      continue;
    }

    if (kvcount == 0) {
      if (more) {
#if DEBUG
        printf("gc got kvcount==0 with more==true\n");
#endif
        continue;
      }
      // reached the end of garbage subspace. wrap around.
      last_key = gc_start_key;
      continue;
    }

    for (int i = 0; i < kvcount; i++) {
      const FDBKeyValue &kv = kvs[i];
      std::vector<uint8_t> key(kv.key, kv.key + kv.key_length);
      process_gc_candidate(key);
      // advance cursor after processing so we don't spin forever on
      // a malformed record that repeatedly fails to clear.
      last_key = std::move(key);
    }
  }
#if DEBUG
  printf("gc done\n");
#endif
  return NULL;
}

bool start_gc() {
  // just in case we restart the gc for some reason
  gc_stop.store(false, std::memory_order_relaxed);

  if (pthread_create(&gc_thread, NULL, garbage_scanner, NULL)) {
    return true;
  }
  return false;
}

void terminate_gc() {
  gc_stop.store(true, std::memory_order_relaxed);
  pthread_join(gc_thread, NULL);
}
