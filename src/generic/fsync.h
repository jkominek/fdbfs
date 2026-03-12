#ifndef __FSYNC_H__
#define __FSYNC_H__

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "util.h"

// ---- Generation barrier manager ----
class FsyncBarrierTable {
private:
  struct Gen;

public:
  // we split inode space into 256 shards. the idea here is that instead of
  // having a single std::map or something storing this stuff per-inode,
  // which would require a big lock everything would contend on, we have a
  // static array of shards, each of which has its own lock. that does mean
  // that fsync will group together every other inode on the same 1/256th of
  // inode space, but, eh.
  static constexpr size_t shards = 256;

  using CompletionFn = std::function<void(int err)>;

  struct Token {
    fdbfs_ino_t ino{};
    std::shared_ptr<Gen> gen;
  };

  explicit FsyncBarrierTable() = default;

  // Called at the conceptual start of an inode-affecting operation.
  Token begin_op(fdbfs_ino_t ino) {
    ShardState &st = get_shard_state(ino);

    for (;;) {
      std::shared_ptr<Gen> g;
      {
        std::lock_guard lk(st.mu);
        g = st.current;
      }

      // Conditional join: only increment if not CLOSED.
      uint64_t s = g->state.load(std::memory_order_acquire);
      if (s & Gen::CLOSED)
        continue;

      if (g->state.compare_exchange_weak(s, s + 1, std::memory_order_acq_rel,
                                         std::memory_order_acquire)) {
        return Token{ino, std::move(g)};
      }
    }
  }

  // Called exactly once when that operation is durably complete for that inode.
  void end_op(Token &t) {
    if (!t.gen)
      return;

    auto g = std::move(t.gen);
    t.gen.reset();

    uint64_t prev = g->state.fetch_sub(1, std::memory_order_acq_rel);
    uint64_t now = prev - 1;

    if (((now & Gen::COUNT_MASK) == 0) && (now & Gen::CLOSED)) {
      maybe_fire_on_drain_(g);
    }
  }

  // Nonblocking fsync registration. Returns immediately.
  // - ino: inode to fsync
  // - on_done: completion callback to run once the barrier has drained.
  void fsync_async(fdbfs_ino_t ino, CompletionFn on_done) {
    assert(on_done);
    ShardState &st = get_shard_state(ino);

    // Fast path: if current generation has no joined ops, reply immediately.
    // This avoids allocating a new generation and avoids any contention.
    {
      std::lock_guard lk(st.mu);
      uint64_t s = st.current->state.load(std::memory_order_acquire);
      if ((s & Gen::COUNT_MASK) == 0 && (s & Gen::CLOSED) == 0) {
        // No joined ops in the current generation at the moment of fsync
        // arrival. Operations that start after this are not required to be
        // waited for.
        on_done(0);
        return;
      }
    }

    // Slow path: cut a new generation and arm the old generation's drain
    // callback.
    std::shared_ptr<Gen> old;
    auto neu = std::make_shared<Gen>();

    // Chaining: start the new gen with a +1 hold representing this fsync.
    neu->state.store(1, std::memory_order_release);

    {
      std::lock_guard lk(st.mu);

      old = st.current;
      st.current = neu;

      // Wire up chaining.
      old->next = neu;

      // Install drain callback before closing. The callback:
      //  1) replies to this fsync
      //  2) releases the +1 hold on the next generation
      //
      // NOTE if we're concerned about being spammed with fsyncs for some
      // reason,
      old->on_drain = [on_done, neu]() mutable {
        on_done(0);

        // Release this fsync's hold on the next generation.
        uint64_t prev = neu->state.fetch_sub(1, std::memory_order_acq_rel);
        uint64_t now = prev - 1;
        if (((now & Gen::COUNT_MASK) == 0) && (now & Gen::CLOSED)) {
          maybe_fire_on_drain_(neu);
        }
      };

      // Close old generation: disallow new joins.
      uint64_t prev_state =
          old->state.fetch_or(Gen::CLOSED, std::memory_order_acq_rel);
      (void)prev_state;
    }

    // If old is already empty, fire immediately (covers the race where it
    // drained between the fast-path check and closure, or it was already empty
    // but closed later).
    uint64_t s = old->state.load(std::memory_order_acquire);
    if (((s & Gen::COUNT_MASK) == 0) && (s & Gen::CLOSED)) {
      maybe_fire_on_drain_(old);
    }
  }

private:
  struct Gen {
    std::atomic<uint64_t> state{0}; // [63]=CLOSED, [0..62]=count
    static constexpr uint64_t CLOSED = 1ull << 63;
    static constexpr uint64_t COUNT_MASK = ~CLOSED;

    std::shared_ptr<Gen> next;

    std::function<void()> on_drain;
    std::atomic<bool> drain_fired{false};
  };

  struct ShardState {
    std::mutex mu;
    std::shared_ptr<Gen> current = std::make_shared<Gen>();
  };

  ShardState shards_[shards];

  // grab the ShardState which corresponds to this inode.
  // the number of inodes which map to a given shard determine
  // the coarseness of fsync
  ShardState &get_shard_state(fdbfs_ino_t ino) {
    return shards_[static_cast<size_t>(ino) & (shards - 1)];
  }

  static void maybe_fire_on_drain_(const std::shared_ptr<Gen> &g) {
    bool expected = false;
    if (!g->drain_fired.compare_exchange_strong(expected, true,
                                                std::memory_order_acq_rel)) {
      return;
    }
    if (g->on_drain)
      g->on_drain();
  }
};

inline FsyncBarrierTable g_fsync_barrier_table;

#endif // __FSYNC_H__
