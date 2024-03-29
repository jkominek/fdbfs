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
 * release
 *************************************************************
 * INITIAL PLAN
 * always succeed.
 *
 * REAL PLAN?
 * not much
 */

extern "C" void fdbfs_release(fuse_req_t req, fuse_ino_t ino,
                              struct fuse_file_info *fi) {
  delete *(extract_fdbfs_filehandle(fi));

  fuse_reply_err(req, 0);
}
