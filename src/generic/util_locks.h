#ifndef __UTIL_LOCKS_H__
#define __UTIL_LOCKS_H__

#include <expected>
#include <functional>
#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdint.h>
#include <sys/file.h>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "util.h"

struct LockConflict {
  pid_t pid;
  short locktype;
  ByteRange range;
};

std::expected<ByteRange, int> flock_to_range(const struct flock *lock);

class LockManagerService {
public:
  LockManagerService();
  ~LockManagerService();

  LockManagerService(const LockManagerService &) = delete;
  LockManagerService &operator=(const LockManagerService &) = delete;
  LockManagerService(LockManagerService &&) = delete;
  LockManagerService &operator=(LockManagerService &&) = delete;

  void queue_lock_manipulation(
      fdbfs_ino_t ino, uint64_t owner, pid_t pid, bool blocking, short locktype,
      ByteRange range,
      std::function<void(std::expected<void, int>)> complete);
  std::optional<LockConflict> query_lock_conflict(fdbfs_ino_t ino,
                                                  uint64_t owner,
                                                  short locktype,
                                                  ByteRange range);

private:
  struct InodeOwnerLockRecord {
    uint64_t owner;
    pid_t pid;
    boost::icl::interval_set<off_t> read_locks;
    boost::icl::interval_set<off_t> write_locks;
  };

  struct LockRequest {
    LockRequest(uint64_t owner, pid_t pid,
                std::function<void(std::expected<void, int>)> complete,
                bool blocking, short locktype, ByteRange range)
        : owner(owner), pid(pid), complete(std::move(complete)),
          blocking(blocking), locktype(locktype), range(range) {};

    uint64_t owner;
    pid_t pid;
    std::function<void(std::expected<void, int>)> complete;
    bool blocking = true;
    short locktype; // F_RDLCK, F_WRLCK, F_UNLCK
    ByteRange range;
  };

  struct InodeLockRecord {
    std::unordered_map<uint64_t, InodeOwnerLockRecord> owner_locks;
    std::shared_mutex owner_locks_mutex;

    // not implemented yet. when we refresh the locks from fdb, we'll
    // flatten them into these two fields and use them for comparison
    boost::icl::interval_set<off_t> fdb_read_locks;
    boost::icl::interval_set<off_t> fdb_write_locks;

    std::list<LockRequest> lock_requests;
  };

  void lock_manager(std::stop_token stop_token);

  std::unordered_set<fdbfs_ino_t> inodes_to_process;
  std::mutex inodes_to_process_mutex;
  std::condition_variable lock_manager_cv;
  std::unordered_map<fdbfs_ino_t, InodeLockRecord> inode_locks;
  std::shared_mutex inode_locks_mutex;
  std::jthread lock_manager_thread;
};

#endif // __UTIL_LOCKS_H__
