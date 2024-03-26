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
 * flush
 *************************************************************
 */

extern "C" void fdbfs_flush(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi) {
  // This is a no-op, we don't maintain buffers, yet.

  fuse_reply_err(req, 0);
}
