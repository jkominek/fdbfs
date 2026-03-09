#ifndef __UTIL_LOCKS_H__
#define __UTIL_LOCKS_H__

#include <optional>
#include <stdint.h>
#include <sys/types.h>

#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>

#include "util.h"

struct LockConflict {
  pid_t pid;
  short locktype;
  ByteRange range;
};

extern bool start_lock_manager();
extern void terminate_lock_manager();
extern void *lock_manager(void *);
extern void queue_lock_manipulation(fuse_req_t req, fuse_ino_t ino,
                                    uint64_t owner, pid_t pid, bool blocking,
                                    short locktype, ByteRange range);
extern std::optional<LockConflict> query_lock_conflict(fuse_ino_t ino,
                                                       uint64_t owner,
                                                       short locktype,
                                                       ByteRange range);

#endif // __UTIL_LOCKS_H__
