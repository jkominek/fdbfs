

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
bool gc_stop = false;

void *garbage_scanner(void *ignore) {
  uint8_t scan_spot = random() & 0xF;
  struct timespec ts;
  ts.tv_sec = 1;
  ts.tv_nsec = 0;
#if DEBUG
  printf("gc starting\n");
#endif
  while (!gc_stop) {
    // TODO vary this or do something to slow things down.
    ts.tv_sec = 1;
    nanosleep(&ts, NULL);

    unique_transaction t = make_transaction();

    // we'll pick a random spot in the garbage space, and
    // then we'll scan a bit past that.
    auto start = key_prefix;
    start.push_back('g');
    auto stop = start;
    uint8_t b = (scan_spot & 0xF) << 4;
    start.push_back(b);
    stop.push_back(b | 0x0F);
    // printf("starting gc at %02x\n", b);
    scan_spot = (scan_spot + 1) % 16;

    unique_future f = wrap_future(fdb_transaction_get_range(
        t.get(), start.data(), start.size(), 0, 1, stop.data(), stop.size(), 0,
        1, 1, 0, FDB_STREAMING_MODE_SMALL, 0, 0,
        // flip between forwards and backwards
        // scanning of our randomly chosen range
        random() & 0x1));
    const FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;
    if (fdb_future_block_until_ready(f.get()) ||
        fdb_future_get_keyvalue_array(f.get(), &kvs, &kvcount, &more) ||
        (kvcount <= 0)) {
      // errors, or nothing to do. take a longer break.
      // sleep extra, though.
      ts.tv_sec = 3;
      nanosleep(&ts, NULL);
      continue;
    }

    if (kvs[0].key_length != inode_key_length) {
      // we found malformed junk in the garbage space. ironic.
      fdb_transaction_clear(t.get(), kvs[0].key, kvs[0].key_length);
      auto g = wrap_future(fdb_transaction_commit(t.get()));
      // if it fails, it fails, we'll try again the next time we
      // stumble across it.
      if (fdb_future_block_until_ready(g.get())) {
        /* nothing to do */;
      }
      continue;
    }

    // ok we found a garbage-collectible inode.
    // fetch the in-use records for it.
    fuse_ino_t ino;
    bcopy(kvs[0].key + key_prefix.size() + 1, &ino, sizeof(fuse_ino_t));
    ino = be64toh(ino);
    if (lookup_count_nonzero(ino)) {
      // we're still using it, don't bother checking the use records
      continue;
    }
    // at this point we shouldn't have a use record in the database, we'd
    // only be finding other hosts' use records for the inode.

#if DEBUG
    printf("found garbage inode %lx\n", ino);
#endif
    start = pack_inode_key(ino);
    start.push_back(0x01);
    stop = pack_inode_key(ino);
    stop.push_back(0x02);

    // scan the use range of the inode
    // TODO we actually need to pull all use records, and compare
    // them against the known live processes
    {
      auto g = wrap_future(fdb_transaction_get_range(
          t.get(), start.data(), start.size(), 0, 1, stop.data(), stop.size(),
          0, 1, 1, 0, FDB_STREAMING_MODE_SMALL, 0, 0, 0));
      if (fdb_future_block_until_ready(g.get()) ||
          fdb_future_get_keyvalue_array(g.get(), &kvs, &kvcount, &more) ||
          kvcount > 0) {
        // welp. nothing to do.
#if DEBUG
        printf("nothing to do on the garbage inode\n");
#endif
        continue;
      }
    }

    // it shouldn't be possible for this to be true, but if it is, that's
    // a problem.
    if (lookup_count_nonzero(ino)) {
      // we've started using it
      // TODO  we're in a very weird situation, we've somehow recently
      // started locally using an inode for which there are no use records
      // despite the fact that it's been waiting for garbage collection.
      // we should at least log this, and possibly attempt to repair the
      // situation (restore a use record?) because another host running
      // the GC will not know about our lookup count, and will delete the
      // inode soon.
      continue;
    }
    // wooo no usage, we get to erase it.
#if DEBUG
    printf("cleaning garbage inode\n");
#endif
    auto garbagekey = pack_garbage_key(ino);
    fdb_transaction_clear(t.get(), garbagekey.data(), garbagekey.size());

    erase_inode(t.get(), ino);
    {
      auto g = wrap_future(fdb_transaction_commit(t.get()));
      // if the commit fails, it doesn't matter. we'll try again later.
      if (fdb_future_block_until_ready(g.get())) {
        printf("error when commiting a garbage collection transaction\n");
      }
    }
  }
#if DEBUG
  printf("gc done\n");
#endif
  return NULL;
}

bool start_gc() {
  gc_stop = false; // just in case we restart the gc for some reason
  if (pthread_create(&gc_thread, NULL, garbage_scanner, NULL)) {
    return true;
  }
  return false;
}

void terminate_gc() {
  gc_stop = true;
  pthread_join(gc_thread, NULL);
}
