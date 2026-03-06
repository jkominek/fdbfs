#include "filehandle.h"
#include "inflight.h"

#include <stdio.h>
#include <stdlib.h>

bool FilehandleSerializer::enqueue_inflight(Inflight *inflight) {
  std::unique_lock lk(mu);
  if (closed) {
    return false;
  }

  queue.push_back(PendingItem{
      .kind = PendingKind::Inflight,
      .inflight = inflight,
      .callback = {},
  });
  maybe_start_next_locked(lk);
  return true;
}

bool FilehandleSerializer::enqueue_barrier(std::function<void()> callback,
                                           bool close_after_enqueue) {
  std::unique_lock lk(mu);
  if (closed) {
    return false;
  }

  queue.push_back(PendingItem{
      .kind = PendingKind::Barrier,
      .inflight = nullptr,
      .callback = std::move(callback),
  });
  if (close_after_enqueue) {
    closed = true;
  }
  maybe_start_next_locked(lk);
  return true;
}

void FilehandleSerializer::on_inflight_done(Inflight *inflight) {
  std::unique_lock lk(mu);
  if (!running || queue.empty()) {
    return;
  }
  // TODO when we're convinced this isn't happening (regularly?)
  // we'll just make it an assert.
  if (queue.front().kind != PendingKind::Inflight ||
      queue.front().inflight != inflight) {
    fprintf(stderr,
            "fdbfs serializer fatal mismatch: this=%p running=%d "
            "queue_size=%zu done_inflight=%p front_kind=%d front_inflight=%p\n",
            static_cast<void *>(this), static_cast<int>(running), queue.size(),
            static_cast<void *>(inflight), static_cast<int>(queue.front().kind),
            static_cast<void *>(queue.front().inflight));
    std::terminate();
  }
  queue.pop_front();
  running = false;
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
  return running;
}

std::size_t FilehandleSerializer::queued_count() const {
  std::scoped_lock lk(mu);
  return queue.size();
}

void FilehandleSerializer::maybe_start_next_locked(
    std::unique_lock<std::mutex> &lk) {
  while (!running && !queue.empty()) {
    auto &next = queue.front();
    if (next.kind == PendingKind::Barrier) {
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

    Inflight *inflight = next.inflight;
    running = true;

    // Assumes Inflight will expose a completion callback hook.
    inflight->set_on_done([this, inflight]() { on_inflight_done(inflight); });

    lk.unlock();
    inflight->start();
    lk.lock();
    return;
  }
}
