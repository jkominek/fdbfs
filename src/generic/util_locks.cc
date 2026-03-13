#include "util_locks.h"

#include <errno.h>
#include <sys/file.h>

#include <expected>
#include <functional>
#include <stdexcept>

namespace {
std::optional<LockConflict>
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
} // namespace

std::optional<LockConflict>
LockManagerService::query_lock_conflict(fdbfs_ino_t ino, uint64_t owner,
                                        short locktype, ByteRange range) {
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

void LockManagerService::queue_lock_manipulation(
    fdbfs_ino_t ino, uint64_t owner, pid_t pid, bool blocking, short locktype,
    ByteRange range, std::function<void(std::expected<void, int>)> complete) {
  std::scoped_lock guard(inode_locks_mutex, inodes_to_process_mutex);
  // NOTE we need to ensure that 'complete' remains a non-blocking, constant
  // time thing, as we'll be calling it inside of a lock.
  LockRequest lockreq(owner, pid, std::move(complete), blocking, locktype,
                      range);
  inode_locks[ino].lock_requests.push_back(std::move(lockreq));
  inodes_to_process.insert(ino);
  lock_manager_cv.notify_one();
}

void LockManagerService::lock_manager(std::stop_token stop_token) {
  std::stop_callback stop_wakeup(
      stop_token, [this]() { lock_manager_cv.notify_all(); });
  fdbfs_set_thread_name("locks");
  for (;;) {
    if (stop_token.stop_requested()) {
      break;
    }

    std::unordered_set<fdbfs_ino_t> to_process;
    {
      std::unique_lock lock(inodes_to_process_mutex);
      lock_manager_cv.wait(
          lock, [&]() { return stop_token.stop_requested() ||
                               !inodes_to_process.empty(); });
      if (stop_token.stop_requested()) {
        break;
      }
      if (inodes_to_process.empty()) {
        // shouldn't happen with the wait predicate, but keep this defensive.
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
            }

            if (!req.blocking) {
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
}

LockManagerService::LockManagerService() {
  try {
    lock_manager_thread =
        std::jthread([this](std::stop_token st) { lock_manager(st); });
  } catch (const std::system_error &) {
    throw std::runtime_error("lock manager thread start failed");
  }
}

LockManagerService::~LockManagerService() {
  if (lock_manager_thread.joinable()) {
    lock_manager_thread.request_stop();
    lock_manager_cv.notify_all();
    lock_manager_thread.join();
  }
}
