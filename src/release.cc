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
  auto fh = extract_fdbfs_filehandle(fi);
  if (!fh) {
    fuse_reply_err(req, EBADF);
    return;
  }

            auto generation = decrement_lookup_count(ino, 1);
            if (generation.has_value()) {
              best_effort_clear_inode_use_record(ino, *generation);
            }
            fuse_reply_err(req, 0);

  free_fdbfs_filehandle_slot(fi);
}
