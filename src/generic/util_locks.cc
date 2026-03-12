#include "util_locks.h"

#include <errno.h>
#include <pthread.h>
#include <sys/file.h>

#include <atomic>
#include <expected>
#include <functional>
#include <list>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

class InodeOwnerLockRecord {
public:
  uint64_t owner;
  pid_t pid;
  boost::icl::interval_set<off_t> read_locks;
  boost::icl::interval_set<off_t> write_locks;
};

class LockRequest {
public:
  LockRequest(uint64_t owner, pid_t pid,
              std::function<void(std::expected<void, int>)> complete,
              bool blocking, short locktype, ByteRange range)
      : owner(owner), pid(pid), complete(complete), blocking(blocking),
        locktype(locktype), range(range) {};

  uint64_t owner;
  pid_t pid;
  std::function<void(std::expected<void, int>)> complete;
  bool blocking = true;
  short locktype; // F_RDLCK, F_WRLCK, F_UNLCK
  ByteRange range;
};

class InodeLockRecord {
public:
  std::unordered_map<uint64_t, InodeOwnerLockRecord> owner_locks;
  std::shared_mutex owner_locks_mutex;

  // not implemented yet. when we refresh the locks from fdb, we'll
  // flatten them into these two fields and use them for comparison
  boost::icl::interval_set<off_t> fdb_read_locks;
  boost::icl::interval_set<off_t> fdb_write_locks;

  std::list<LockRequest> lock_requests;
};

std::unordered_set<fdbfs_ino_t> inodes_to_process;
std::mutex inodes_to_process_mutex;
std::unordered_map<fdbfs_ino_t, InodeLockRecord> inode_locks;
std::shared_mutex inode_locks_mutex;

std::atomic<bool> ready{false};
std::atomic<bool> lock_manager_stop{false};
pthread_t lock_manager_thread;
bool lock_manager_started = false;

static std::optional<LockConflict>
find_conflict_for_owner_lock(pid_t pid, short locktype,
                             const boost::icl::interval_set<off_t> &held_locks,
                             ByteRange requested_range) {
  for (const auto &held_range : held_locks) {
    if (!boost::icl::intersects(held_range, requested_range)) {
      continue;
    }
    return LockConflict{.pid = pid, .locktype = locktype, .range = held_range};
  }
  return std::nullopt;
}

std::optional<LockConflict> query_lock_conflict(fdbfs_ino_t ino, uint64_t owner,
                                                short locktype,
                                                ByteRange range) {
  if (locktype == F_UNLCK) {
    return std::nullopt;
  }

  std::shared_lock inode_locks_lock(inode_locks_mutex);
  auto inode_it = inode_locks.find(ino);
  if (inode_it == inode_locks.end()) {
    return std::nullopt;
  }

  InodeLockRecord &inode_lock = inode_it->second;
  std::shared_lock owner_locks_lock(inode_lock.owner_locks_mutex);
  for (const auto &[other_owner, other_owner_locks] : inode_lock.owner_locks) {
    if (other_owner == owner) {
      continue;
    }

    if (locktype == F_RDLCK) {
      auto conflict = find_conflict_for_owner_lock(
          other_owner_locks.pid, F_WRLCK, other_owner_locks.write_locks, range);
      if (conflict.has_value()) {
        return conflict;
      }
    } else if (locktype == F_WRLCK) {
      auto read_conflict = find_conflict_for_owner_lock(
          other_owner_locks.pid, F_RDLCK, other_owner_locks.read_locks, range);
      if (read_conflict.has_value()) {
        return read_conflict;
      }
      auto write_conflict = find_conflict_for_owner_lock(
          other_owner_locks.pid, F_WRLCK, other_owner_locks.write_locks, range);
      if (write_conflict.has_value()) {
        return write_conflict;
      }
    }
  }

  // TODO scan the network locks; not sure how to populate them yet
  // prefer scanning our local locks, because we can return a pid
  // in those cases. any pid for the network locks will be made up.

  return std::nullopt;
}

void queue_lock_manipulation(
    fdbfs_ino_t ino, uint64_t owner, pid_t pid, bool blocking, short locktype,
    ByteRange range, std::function<void(std::expected<void, int>)> complete) {
  {
    std::scoped_lock guard(inode_locks_mutex, inodes_to_process_mutex);
    // NOTE we need to ensure that 'complete' remains a non-blocking, constant
    // time thing, as we'll be calling it inside of a lock.
    LockRequest lockreq(owner, pid, std::move(complete), blocking, locktype,
                        range);
    inode_locks[ino].lock_requests.push_back(std::move(lockreq));
    inodes_to_process.insert(ino);

    if (!ready.exchange(true, std::memory_order_release)) {
      ready.notify_one();
    }
  }
}

void *lock_manager(void *ignore) {
  (void)ignore;
  fdbfs_set_thread_name("locks");
  for (;;) {
    if (lock_manager_stop.load(std::memory_order_relaxed)) {
      break;
    }
    if (!ready.exchange(false, std::memory_order_acquire)) {
      ready.wait(false, std::memory_order_relaxed);
      continue;
    }
    if (lock_manager_stop.load(std::memory_order_relaxed)) {
      break;
    }

    std::unordered_set<fdbfs_ino_t> to_process;
    {
      std::scoped_lock lock(inodes_to_process_mutex);
      if (inodes_to_process.empty()) {
        // pretty unlikely, but we can just bail now if
        // there is no work to do.
        continue;
      }
      to_process = inodes_to_process;
      inodes_to_process.clear();
    }

    {
      std::shared_lock lock(inode_locks_mutex);
      for (fdbfs_ino_t ino : to_process) {
        auto it = inode_locks.find(ino);
        if (it == inode_locks.end()) {
          continue;
        }

        InodeLockRecord &inode_lock = it->second;
        std::unique_lock owner_locks_lock(inode_lock.owner_locks_mutex);

        // scan the list and process all of the unlocks. maybe
        // we should separate locks and unlocks into different lists.
        for (auto req_it = inode_lock.lock_requests.begin();
             req_it != inode_lock.lock_requests.end();) {
          LockRequest &req = *req_it;

          if (req.locktype == F_UNLCK) {
            // we can always process unlocks... if the lock exists
            auto owner_it = inode_lock.owner_locks.find(req.owner);
            if (owner_it != inode_lock.owner_locks.end()) {
              owner_it->second.read_locks -= req.range;
              owner_it->second.write_locks -= req.range;
              if (owner_it->second.read_locks.empty() &&
                  owner_it->second.write_locks.empty()) {
                inode_lock.owner_locks.erase(owner_it);
              }
            }
            req.complete({});
            req_it = inode_lock.lock_requests.erase(req_it);
            continue;
          }

          req_it++;
        }

        bool reprocess = false;
        do {
          reprocess = false;
          for (auto req_it = inode_lock.lock_requests.begin();
               req_it != inode_lock.lock_requests.end();) {
            LockRequest &req = *req_it;

            // okay can we grant this request?
            bool grantable = true;
            for (const auto &[owner, owner_lock] : inode_lock.owner_locks) {
              if (owner == req.owner) {
                continue;
              }

              if (req.locktype == F_RDLCK) {
                if (boost::icl::intersects(owner_lock.write_locks, req.range)) {
                  grantable = false;
                  break;
                }
                continue;
              }

              if (req.locktype == F_WRLCK &&
                  (boost::icl::intersects(owner_lock.read_locks, req.range) ||
                   boost::icl::intersects(owner_lock.write_locks, req.range))) {
                grantable = false;
                break;
              }
            }

            if (grantable) {
              auto [owner_it, inserted] =
                  inode_lock.owner_locks.try_emplace(req.owner);
              if (inserted) {
                owner_it->second.owner = req.owner;
                owner_it->second.pid = req.pid;
              }
              if (req.locktype == F_RDLCK) {
                if (boost::icl::intersects(owner_it->second.write_locks,
                                           req.range)) {
                  owner_it->second.write_locks -= req.range;
                  reprocess = true;
                }
                owner_it->second.read_locks += req.range;
              } else if (req.locktype == F_WRLCK) {
                owner_it->second.read_locks -= req.range;
                owner_it->second.write_locks += req.range;
              }
              req.complete({});
              req_it = inode_lock.lock_requests.erase(req_it);
              continue;
            } else if (!req.blocking) {
              req.complete(std::unexpected(EAGAIN));
              req_it = inode_lock.lock_requests.erase(req_it);
              continue;
            }
            // blocking requests just stay in the queue to be rechecked later

            ++req_it;
          }
        } while (reprocess);
      }
    }
  }
  return NULL;
}

bool start_lock_manager() {
  lock_manager_stop.store(false, std::memory_order_relaxed);
  if (pthread_create(&lock_manager_thread, NULL, lock_manager, NULL)) {
    return true;
  }
  lock_manager_started = true;
  return false;
}

void terminate_lock_manager() {
  if (!lock_manager_started) {
    return;
  }
  lock_manager_started = false;

  lock_manager_stop.store(true, std::memory_order_relaxed);
  if (!ready.exchange(true, std::memory_order_release)) {
    ready.notify_one();
  }

  pthread_join(lock_manager_thread, NULL);
}
