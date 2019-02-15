
#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 600
#include <foundationdb/fdb_c.h>

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
 * read
 *************************************************************
 * INITIAL PLAN
 * for v0, we'll issue either a single get, or a single range
 * read, and call it good.
 *
 * that makes this relatively straightforward to implement.
 *
 * REAL PLAN?
 * in reality we'll probably also need to read the file attributes
 * and pick up the file size, in addition to the content reads.
 *
 * that still isn't too bad, but it means that we'll have to
 * handle multiple futures in the end.
 */

struct fdbfs_inflight_read {
  struct fdbfs_inflight_base base;
  fuse_ino_t ino;
  size_t size;
  off_t off;
};

void fdbfs_read_callback(FDBFuture *f, void *p)
{

}

void fdbfs_read_issuer(void *p)
{
  // issue fdb_transaction_get_range for appropriate content.
  // point the resulting future at fdbfs_read_callback

  fdb_future_set_callback(NULL, fdbfs_error_checker, p);
}

void fdbfs_read(fuse_req_t req, fuse_ino_t ino, size_t size,
		off_t off, struct fuse_file_info *fi)
{
  // given inode, figure out the appropriate key range, and
  // start reading it, filling it into a buffer to be sent back
  // with fuse_reply_buf
  struct fdbfs_inflight_read *inflight;
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_read),
				   req,
				   fdbfs_read_callback,
				   fdbfs_read_issuer);

  inflight->ino = ino;
  inflight->size = size;
  inflight->off = off;

  fdbfs_read_issuer(inflight);
}
