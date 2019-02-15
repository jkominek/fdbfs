#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>

#include "util.h"
#include "inflight.h"

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

void fdbfs_open(fuse_req_t req, fuse_ino_t ino,
                struct fuse_file_info *fi)
{
  // for now maybe just always succeed?

  // real implementation will need to mark the inode as opened
  // until release is called.

  // that'll prevent collection of the inode and associated data
}
