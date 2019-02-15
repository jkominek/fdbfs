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
 * readdir
 *************************************************************
 * INITIAL PLAN
 * ?
 *
 * REAL PLAN
 * ?
 */

void fdbfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                   off_t off, struct fuse_file_info *fi)
{
  // Read a range of entries under the directory inode

  // fsprefix inode
  // fsprefix inode \x00 AAA
  // fsprefix inode \x00 foo
  // fsprefix inode \x00 xxx

  // range is "fsprefix inode \x00\x00" to "fsprefix inode \x00\xff"
  
  // Pack them into a buffer with fuse_add_direntry

  // set future callback to return them with fuse_reply_buf
}
