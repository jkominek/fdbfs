#ifndef __UTIL_LOCKS_H__
#define __UTIL_LOCKS_H__

#include <functional>
#include <optional>
#include <stdint.h>
#include <sys/types.h>
#include <expected>

#include "util.h"

struct LockConflict {
  pid_t pid;
  short locktype;
  ByteRange range;
};

extern bool start_lock_manager();
extern void terminate_lock_manager();
extern void *lock_manager(void *);
extern void queue_lock_manipulation(
    fdbfs_ino_t ino, uint64_t owner, pid_t pid, bool blocking, short locktype,
    ByteRange range,
    std::function<void(std::expected<void, int>)> complete);
extern std::optional<LockConflict> query_lock_conflict(fdbfs_ino_t ino,
                                                       uint64_t owner,
                                                       short locktype,
                                                       ByteRange range);

#endif // __UTIL_LOCKS_H__
