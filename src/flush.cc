#include <assert.h>
#include <errno.h>
#include <fcntl.h>
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
  if (!fh->serializer.enqueue_barrier(
          [req, fh]() { fuse_reply_err(req, 0); })) {
    fuse_reply_err(req, EBADF);
    return;
  }
}
