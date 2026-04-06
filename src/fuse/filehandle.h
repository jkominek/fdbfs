#ifndef __FILEHANDLE_H__
#define __FILEHANDLE_H__

#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>

#include <cstddef>
#include <ctime>
#include <deque>
#include <functional>
#include <limits>
#include <mutex>
#include <unordered_map>

#include "generic/util.h"

class FuseInflightAction;
template <typename ActionT> class InflightT;

// Per-filehandle state used to enforce request ordering for operations that
// share a FUSE file handle.
class FileHandle {
public:
  explicit FileHandle(fuse_ino_t ino) : ino(ino) {}
  ~FileHandle() = default;

  FileHandle(const FileHandle &) = delete;
  FileHandle &operator=(const FileHandle &) = delete;
  FileHandle(FileHandle &&) = delete;
  FileHandle &operator=(FileHandle &&) = delete;

  // Enqueue an inflight that should run under this filehandle.
  [[nodiscard]] bool
  enqueue_inflight(InflightT<FuseInflightAction> *inflight,
                   std::optional<ByteRange> range =
                       ByteRange::closed(0, std::numeric_limits<off_t>::max()));

  // Enqueue a non-inflight barrier callback (e.g. flush/release drain marker).
  // When close_after_enqueue is true, no further enqueue operations are
  // accepted after this barrier is inserted.
  [[nodiscard]] bool enqueue_barrier(std::function<void(int)> callback,
                                     bool close_after_enqueue = false);

  // Called by inflight completion path to release the current slot.
  void on_inflight_done(InflightT<FuseInflightAction> *inflight, int err);

  // Prevent further enqueue operations.
  void close();

  [[nodiscard]] bool is_closed() const;
  [[nodiscard]] bool is_running() const;
  [[nodiscard]] std::size_t queued_count() const;
  [[nodiscard]] fuse_ino_t inode() const { return ino; }
  bool do_atime = true;

  // latest (unstored) read time, for setting atime
  std::optional<struct timespec> latest_read;

private:
  enum class PendingKind { Inflight, Barrier };

  struct PendingItem {
    PendingKind kind;
    InflightT<FuseInflightAction> *inflight = nullptr;
    std::function<void(int)> callback;
    bool readonly;
    ByteRange range = ByteRange::closed(0, std::numeric_limits<off_t>::max());
  };

  // Starts or dispatches the next queued item if idle.
  void maybe_start_next_locked(std::unique_lock<std::mutex> &lk);
  void maybe_start_atime_update_locked(std::unique_lock<std::mutex> &lk);

  mutable std::mutex mu;
  std::deque<PendingItem> queue;
  std::unordered_map<InflightT<FuseInflightAction> *, PendingItem> active;

  boost::icl::split_interval_map<off_t, int> inflight_reads;
  boost::icl::split_interval_map<off_t, int> inflight_writes;

  const fuse_ino_t ino;
  bool closed = false;
  // error from operations we've serialized, for flush/close
  int stored_error = 0;
  bool atime_update_needed = false;
  bool atime_update_running = false;
};

#endif
