#ifndef __FILEHANDLE_H__
#define __FILEHANDLE_H__

#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>

#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>

class Inflight;

// Per-filehandle serializer used to enforce request ordering for operations
// that share a FUSE file handle.
class FilehandleSerializer {
public:
  explicit FilehandleSerializer(fuse_ino_t ino) : ino(ino) {}
  ~FilehandleSerializer() = default;

  FilehandleSerializer(const FilehandleSerializer &) = delete;
  FilehandleSerializer &operator=(const FilehandleSerializer &) = delete;
  FilehandleSerializer(FilehandleSerializer &&) = delete;
  FilehandleSerializer &operator=(FilehandleSerializer &&) = delete;

  // Enqueue an inflight that should run under this serializer.
  [[nodiscard]] bool enqueue_inflight(Inflight *inflight);

  // Enqueue a non-inflight barrier callback (e.g. flush/release drain marker).
  // When close_after_enqueue is true, no further enqueue operations are
  // accepted after this barrier is inserted.
  [[nodiscard]] bool enqueue_barrier(std::function<void()> callback,
                                     bool close_after_enqueue = false);

  // Called by inflight completion path to release the current slot.
  void on_inflight_done(Inflight *inflight);

  // Prevent further enqueue operations.
  void close();

  [[nodiscard]] bool is_closed() const;
  [[nodiscard]] bool is_running() const;
  [[nodiscard]] std::size_t queued_count() const;
  [[nodiscard]] fuse_ino_t inode() const { return ino; }

private:
  enum class PendingKind { Inflight, Barrier };

  struct PendingItem {
    PendingKind kind;
    Inflight *inflight = nullptr;
    std::function<void()> callback;
  };

  // Starts or dispatches the next queued item if idle.
  void maybe_start_next_locked(std::unique_lock<std::mutex> &lk);

  mutable std::mutex mu;
  std::deque<PendingItem> queue;
  const fuse_ino_t ino;
  bool running = false;
  bool closed = false;
};

#endif
