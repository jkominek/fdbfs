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

#include "util.h"

class FuseInflightAction;
template <typename ActionT> class InflightT;

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
  [[nodiscard]] bool
  enqueue_inflight(InflightT<FuseInflightAction> *inflight,
                   std::optional<ByteRange> range =
                       ByteRange::closed(0, std::numeric_limits<off_t>::max()));

  // Enqueue a non-inflight barrier callback (e.g. flush/release drain marker).
  // When close_after_enqueue is true, no further enqueue operations are
  // accepted after this barrier is inserted.
  [[nodiscard]] bool enqueue_barrier(std::function<void()> callback,
                                     bool close_after_enqueue = false);

  // Called by inflight completion path to release the current slot.
  void on_inflight_done(InflightT<FuseInflightAction> *inflight);

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
    InflightT<FuseInflightAction> *inflight = nullptr;
    std::function<void()> callback;
    bool readonly;
    ByteRange range = ByteRange::closed(0, std::numeric_limits<off_t>::max());
  };

  // Starts or dispatches the next queued item if idle.
  void maybe_start_next_locked(std::unique_lock<std::mutex> &lk);

  mutable std::mutex mu;
  std::deque<PendingItem> queue;
  std::unordered_map<InflightT<FuseInflightAction> *, PendingItem> active;

  boost::icl::split_interval_map<off_t, int> inflight_reads;
  boost::icl::split_interval_map<off_t, int> inflight_writes;

  const fuse_ino_t ino;
  bool closed = false;
};

struct fdbfs_filehandle {
  explicit fdbfs_filehandle(fuse_ino_t ino, bool atime)
      : atime(atime), atime_update_needed(false), serializer(ino) {}

  // TODO include a 'noatime' flag, possibly an enum with multiple settings
  // such as: none, normal, rel, lazy.
  // we also need to track the latest atime here, along with whatever Inflight
  // is trying to update the atime. only want to try running one at a time
  // per inode, since we've got to read and then write the inode to update the
  // atime, there's an opportunity for multiple reads to finish, incrementing
  // the atime here before we get around to applying it to fdb. we'll need
  // to be able to lock the value.

  // immutable; if true, update atime field when appropriate.
  bool atime;
  // take before manipulating any of the atime fields
  std::mutex atime_mutex;
  bool atime_update_needed;
  // the time of our most recent access. when updating the database,
  // wait to take the lock and read this for as long as possible.
  struct timespec atime_target;
  // update this whenever we're sure the atime is a larger value.
  // other systems might dramatically increase the atime, at which point
  // we can avoid wasting our time attempting updates.
  struct timespec atime_last_known;
  FilehandleSerializer serializer;
};

#endif
