#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fdbfs_ops.h"
#include "filehandle.h"
#include "inflight.h"
#include "util.h"
#include "util_locks.h"

/*************************************************************
 * flush
 *************************************************************
 * Insert a barrier into the FilehandleSerializer which will
 * reply to FUSE indicating that all of the operations have been
 * flushed.
 */

extern "C" void fdbfs_flush(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi) {
  auto fh = extract_fdbfs_filehandle(fi);
  if (!fh) {
    fuse_reply_err(req, EBADF);
    return;
  }

  // construct the appropriate callback for the barrier
  std::function<void()> barrier_callback;
  if (fi->lock_owner) {
    auto lock_owner = fi->lock_owner;
    barrier_callback = [req, ino, lock_owner]() {
      ByteRange range(0, std::numeric_limits<off_t>::max());
      queue_lock_manipulation(req, ino, lock_owner, 0, 0, F_UNLCK, range);
    };
  } else {
    barrier_callback = [req]() { fuse_reply_err(req, 0); };
  }

  if (!fh->serializer.enqueue_barrier(barrier_callback)) {
    fuse_reply_err(req, EBADF);
    return;
  }
}
