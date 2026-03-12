#include <boost/icl/concept/interval.hpp>
#include <errno.h>
#include <expected>
#include <fcntl.h>
#include <inttypes.h>
#include <limits>
#include <stdio.h>

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
