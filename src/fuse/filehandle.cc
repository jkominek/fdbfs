#include "filehandle.h"
#include "fuse_inflight_action.h"

#include <stdio.h>
#include <stdlib.h>

bool FilehandleSerializer::enqueue_inflight(
    InflightT<FuseInflightAction> *inflight,
                                            std::optional<ByteRange> range) {
  std::unique_lock lk(mu);
  if (closed) {
    return false;
  }

  auto pi =
      PendingItem{.kind = PendingKind::Inflight,
                  .inflight = inflight,
                  .callback = {},
                  .readonly = inflight->read_write() == ReadWrite::ReadOnly};
  if (range.has_value())
    pi.range = *range;
  queue.push_back(std::move(pi));
  maybe_start_next_locked(lk);
  return true;
}

bool FilehandleSerializer::enqueue_barrier(std::function<void()> callback,
                                           bool close_after_enqueue) {
  std::unique_lock lk(mu);
  if (closed) {
    return false;
  }

  queue.push_back(PendingItem{.kind = PendingKind::Barrier,
                              .inflight = nullptr,
                              .callback = std::move(callback),
                              .readonly = true});
  if (close_after_enqueue) {
    closed = true;
  }
  maybe_start_next_locked(lk);
  return true;
}

void FilehandleSerializer::on_inflight_done(
    InflightT<FuseInflightAction> *inflight) {
  std::unique_lock lk(mu);
  auto it = active.find(inflight);
  assert(it != active.end());

  if (it->second.readonly) {
    inflight_reads -= std::make_pair(it->second.range, 1);
  } else {
    inflight_writes -= std::make_pair(it->second.range, 1);
  }

  active.erase(it);
  maybe_start_next_locked(lk);
}

void FilehandleSerializer::close() {
  std::scoped_lock lk(mu);
  closed = true;
}

bool FilehandleSerializer::is_closed() const {
  std::scoped_lock lk(mu);
  return closed;
}

bool FilehandleSerializer::is_running() const {
  std::scoped_lock lk(mu);
  return !active.empty();
}

std::size_t FilehandleSerializer::queued_count() const {
  std::scoped_lock lk(mu);
  return queue.size();
}

void FilehandleSerializer::maybe_start_next_locked(
    std::unique_lock<std::mutex> &lk) {
  while (!queue.empty()) {
    auto &next = queue.front();
    if (next.kind == PendingKind::Barrier) {
      // Barriers only run once all currently-active operations have drained.
      if (!active.empty()) {
        return;
      }
      auto cb = std::move(next.callback);
      queue.pop_front();

      lk.unlock();
      if (cb) {
        cb();
      }
      lk.lock();
      continue;
    }

    if (next.inflight == nullptr) {
      queue.pop_front();
      continue;
    }

    InflightT<FuseInflightAction> *inflight = next.inflight;
    // Keep queue order: stop dispatch as soon as the front item conflicts.
    if (next.readonly) {
      // read/read overlap is allowed; read/write overlap is not.
      if (boost::icl::intersects(inflight_writes, next.range)) {
        return;
      }
    } else {
      // write conflicts with both reads and writes.
      if (boost::icl::intersects(inflight_reads, next.range) ||
          boost::icl::intersects(inflight_writes, next.range)) {
        return;
      }
    }

    auto [it, inserted] = active.emplace(inflight, next);
    if (!inserted) {
      // duplicate pointer in active set is a serializer logic error.
      std::terminate();
    }

    if (it->second.readonly) {
      inflight_reads += std::make_pair(it->second.range, 1);
    } else {
      inflight_writes += std::make_pair(it->second.range, 1);
    }
    queue.pop_front();

    // Assumes Inflight will expose a completion callback hook.
    inflight->set_on_done([this, inflight]() { on_inflight_done(inflight); });

    lk.unlock();
    inflight->start();
    lk.lock();
    // Continue dispatching until front item conflicts.
  }
}
