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
 * we'll need to increment/decrement some counter so that the
 * inode contents will be preserved after unlink until everyone
 * is done with them.
 */

extern "C" void fdbfs_open(fuse_req_t req, fuse_ino_t ino,
			   struct fuse_file_info *fi)
{
  fuse_reply_open(req, fi);
}
