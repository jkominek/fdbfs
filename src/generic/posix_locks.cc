#include <boost/icl/concept/interval.hpp>
#include <errno.h>
#include <expected>
#include <fcntl.h>
#include <inttypes.h>
#include <limits>
#include <stdio.h>

#include "fdbfs_ops.h"
#include "util_locks.h"

/*************************************************************
 * POSIX locks (getlk / setlk)
 *************************************************************
 * Stubbed for now.
 */

static std::expected<ByteRange, int> flock_to_range(const struct flock *lock) {
  // FUSE won't generate SEEK_CUR or SEEK_END, it only sends SEEK_SET

  off_t start = lock->l_start;
  off_t end_inclusive = 0;
  if (lock->l_len == 0) {
    end_inclusive = std::numeric_limits<off_t>::max();
  } else if (lock->l_len > 0) {
    const off_t len_minus_one = lock->l_len - 1;
    if ((start > 0) &&
        (len_minus_one > (std::numeric_limits<off_t>::max() - start))) {
      return std::unexpected(EINVAL);
    }
    end_inclusive = start + len_minus_one;
  } else { // lock->l_len < 0
    if (lock->l_len == std::numeric_limits<off_t>::min()) {
      return std::unexpected(EINVAL);
    }
    const off_t len_abs = -lock->l_len;
    if (start < len_abs) {
      return std::unexpected(EINVAL);
    }
    start = start + lock->l_len;
    end_inclusive = lock->l_start - 1;
  }

  if ((start < 0) || (end_inclusive < start)) {
    return std::unexpected(EINVAL);
  }

  return ByteRange::closed(start, end_inclusive);
}

extern "C" void fdbfs_getlk(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi, struct flock *lock) {
  auto range = flock_to_range(lock);
  if (!range.has_value()) {
    fuse_reply_err(req, range.error());
    return;
  }

  auto conflict =
      query_lock_conflict(ino, fi->lock_owner, lock->l_type, range.value());
  if (!conflict.has_value()) {
    lock->l_type = F_UNLCK;
    lock->l_whence = SEEK_SET;
    lock->l_start = 0;
    lock->l_len = 0;
    lock->l_pid = 0;
    fuse_reply_lock(req, lock);
    return;
  }

  const off_t conflict_start = boost::icl::first(conflict->range);
  const off_t conflict_last = boost::icl::last(conflict->range);
  if ((conflict_start < 0) || (conflict_last < conflict_start)) {
    fuse_reply_err(req, EIO);
    return;
  }

  lock->l_type = conflict->locktype;
  lock->l_whence = SEEK_SET;
  lock->l_start = conflict_start;
  if (conflict_last == std::numeric_limits<off_t>::max()) {
    lock->l_len = 0;
  } else {
    lock->l_len = (conflict_last - conflict_start) + 1;
  }
  lock->l_pid = conflict->pid;
  fuse_reply_lock(req, lock);
}

extern "C" void fdbfs_setlk(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi, struct flock *lock,
                            int sleep) {
  if ((fi == NULL) || (lock == NULL)) {
    fuse_reply_err(req, EINVAL);
    return;
  }

  auto range = flock_to_range(lock);
  if (!range.has_value()) {
    fuse_reply_err(req, range.error());
    return;
  }

  queue_lock_manipulation(req, ino, fi->lock_owner, lock->l_pid, sleep != 0,
                          lock->l_type, range.value());
}

#if 0
extern "C" void fdbfs_setlk(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi, struct flock *lock,
                            int sleep) {
  const auto lock_type_str = [](short t) -> const char * {
    switch (t) {
    case F_RDLCK:
      return "RDLCK";
    case F_WRLCK:
      return "WRLCK";
    case F_UNLCK:
      return "UNLCK";
    default:
      return "?";
    }
  };
  const auto whence_str = [](short w) -> const char * {
    switch (w) {
    case SEEK_SET:
      return "SET";
    case SEEK_CUR:
      return "CUR";
    case SEEK_END:
      return "END";
    default:
      return "?";
    }
  };

  if (lock != NULL) {
    fprintf(
        stdout,
        "fdbfs setlk ino=0x%" PRIx64 " owner=0x%" PRIx64
        " sleep=%d type=%s(%d) whence=%s(%d) start=%lld len=%lld pid=0x%lx\n",
        static_cast<uint64_t>(ino), (fi != NULL) ? fi->lock_owner : 0, sleep,
        lock_type_str(lock->l_type), static_cast<int>(lock->l_type),
        whence_str(lock->l_whence), static_cast<int>(lock->l_whence),
        static_cast<long long>(lock->l_start),
        static_cast<long long>(lock->l_len),
        static_cast<unsigned long>(lock->l_pid));
  } else {
    fprintf(stdout,
            "fdbfs setlk ino=0x%" PRIx64 " owner=0x%" PRIx64
            " sleep=%d lock=<null>\n",
            static_cast<uint64_t>(ino), (fi != NULL) ? fi->lock_owner : 0,
            sleep);
  }
  fflush(stdout);

  fuse_reply_err(req, 0);
}
#endif
