#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fdbfs_ops.h"
#include "inflight.h"
#include "util.h"

/*************************************************************
 * open
 *************************************************************
 * INITIAL PLAN
 * always succeed.
 *
 * REAL PLAN?
 * set up our internal structure.
 * update local lookup/use tracking in open/release too.
 */

extern "C" void fdbfs_open(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info *fi) {
  if (fi->flags & O_TRUNC) {
    // truncate and time updates are handled through setattr logic.
    fdbfs_setattr_open_trunc(req, ino, fi);
    return;
  }

  // TODO any other flags we're passed that we could handle?
  if (fi->flags & O_APPEND) {
    // TODO need to test to see how this behaves with writes
  }
  (void)reply_open_with_handle(req, ino, fi);
}
