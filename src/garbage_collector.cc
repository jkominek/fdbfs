

#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "inflight.h"
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

  auto start = pack_inode_key(ino);
  start.push_back(INODE_USE_PREFIX);
  auto stop = pack_inode_key(ino);
  stop.push_back(INODE_USE_PREFIX + 1);

  // scan the use range of the inode
  // TODO we actually need to pull all use records, and compare
  // them against the known live processes
  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  {
    // NOTE the below is a little uncertain because the liveness checker
    //      isn't quite finalized.
    // TODO scan ALL of the use records using small batches, looping if
    //      more==true, because we _expect_ that the record is still in use
    //      and that there won't be anything to do.
    // - if we encounter one which corresponds to a known live host,
    //   then the GC record is valid and we can return early.
    // - if we encounter ones which correspond to known DEAD hosts,
    //   then we can delete them and save ourselves effort in the future.
    // - at the end, if there are no use records (either because it started
    //   like that, or because we deleted them all), then we can process
    //   the record as though it isn't in use (because it isn't)
    // - if we're left with use records where we're not sure if the host
    //   is alive or dead, then we should pester the liveness checker to
    //   figure that out.
    auto f = wrap_future(fdb_transaction_get_range(
        t.get(), start.data(), start.size(), 0, 1, stop.data(), stop.size(), 0,
        1, 1, 0, FDB_STREAMING_MODE_SMALL, 0, 0, 0));
    if (fdb_future_block_until_ready(f.get()) ||
        fdb_future_get_keyvalue_array(f.get(), &kvs, &kvcount, &more) ||
        kvcount > 0) {
      // use records present (or error), nothing to do.
#if DEBUG
      printf("nothing to do on the garbage inode\n");
#endif
      return;
    }
  }

  // it shouldn't be possible for this to be true, but if it is, that's
  // a problem.
  if (lookup_count_nonzero(ino)) {
    // we've started using the inode
    // TODO we're in a very weird situation, we've somehow recently
    // started locally using an inode for which there are no use records
    // despite the fact that it's been waiting for garbage collection.
    return;
  }

  // no usage, erase it.
#if DEBUG
  printf("cleaning garbage inode\n");
#endif
  fdb_transaction_clear(t.get(), garbage_key.data(), garbage_key.size());
  // NOTE we could be paranoid and fetch the inode and doublecheck that
  // nlinks==0 and there are no dirents
  erase_inode(t.get(), ino);

  auto f = wrap_future(fdb_transaction_commit(t.get()));
  // on failure, we can only proceed and try again in a future scan.
  const fdb_error_t wait_err = fdb_future_block_until_ready(f.get());
  (void)wait_err;
}

void *garbage_scanner(void *ignore) {
  struct timespec ts;
  auto gc_start_key = key_prefix;
  gc_start_key.push_back(GARBAGE_PREFIX);
  auto gc_stop_key = key_prefix;
  gc_stop_key.push_back(GARBAGE_PREFIX + 1);
  std::vector<uint8_t> last_key = gc_start_key;
#if DEBUG
  printf("gc starting\n");
#endif
  while (!gc_stop.load(std::memory_order_relaxed)) {
    // TODO vary this or do something to slow things down.
    ts.tv_sec = 1;
    ts.tv_nsec = 0;
    nanosleep(&ts, NULL);

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
