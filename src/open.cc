#include <assert.h>
#include <errno.h>
#define _GNU_SOURCE
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
 * in-use records are handled by lookup & forget, not open & release.
 */

extern "C" void fdbfs_open(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info *fi) {
  struct fdbfs_filehandle *fh = new fdbfs_filehandle;
  fh->atime_update_needed = false;

  *(extract_fdbfs_filehandle(fi)) = fh;

  // TODO any other flags we're passed that we could handle?
  if (fi->flags & O_APPEND) {
    // TODO need to test to see how this behaves with writes
  }
#ifdef O_NOATIME
  if (fi->flags & O_NOATIME) {
    fh->atime = false;
  } else {
    fh->atime = true;
  }
#else
  fh->atime = true;
#endif
  if (fi->flags & O_TRUNC) {
    // TODO run setattr truncate code, including time changes
  }

  if (fuse_reply_open(req, fi) < 0) {
    // had some sort of error, so release will never be called, we'll leak
    // memory if we don't clean up right now.
    delete fh;
  }
}
