#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>

#include "util.h"
#include "inflight.h"
#include "fdbfs_ops.h"

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
			   struct fuse_file_info *fi)
{
  struct fdbfs_filehandle *fh = new fdbfs_filehandle;

  *(extract_fdbfs_filehandle(fi)) = fh;

  fuse_reply_open(req, fi);
}
