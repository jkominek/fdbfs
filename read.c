
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
  uint16_t data_prefix_len;
  size_t size;
  off_t off;
};

void fdbfs_read_callback(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_read *inflight = p;

  FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;

  fdb_future_get_keyvalue_array(f, (const FDBKeyValue **)&kvs, &kvcount, &more);

  char buffer[inflight->size];
  
  for(int i=0; i<kvcount; i++) {
    FDBKeyValue kv = kvs[i];
    uint64_t block;
    bcopy(((uint8_t*)kv.key) + inflight->data_prefix_len,
	  &block,
	  sizeof(uint64_t));
    // TODO variable block size
    uint64_t block_off = inflight->off - block * 8192;
    if( (block * 8192) < inflight->off ) {
      // start our read offset into the block
      // we're definitionally at the start, so no offset into buffer needed.
      bcopy(kv.value + block_off,
	    buffer,
	    min(inflight->size, 8192 - block_off));
    } else {
      int position = block * 8192 - inflight->off;
      bcopy(kv.value,
	    buffer + position,
	    min(inflight->size - position, 8192));
    }
  }

  fuse_reply_buf(inflight->base.req, buffer, inflight->size);
}

void fdbfs_read_issuer(void *p)
{
  struct fdbfs_inflight_read *inflight;
  inflight = (struct fdbfs_inflight_read *)p;

  uint8_t start[512], stop[512];
  int len;
  pack_inode_key(inflight->ino, start, &len);
  start[len++] = 'f';
  // TODO variable block size
  uint64_t start_block = (inflight->off >> 13);
  uint64_t stop_block  = ((inflight->off + inflight->size) >> 13);
  bcopy(&start_block, start+len, sizeof(uint64_t));
  bcopy(&stop_block, stop+len, sizeof(uint64_t));
  inflight->data_prefix_len = len;
  len += sizeof(uint64_t);

  FDBFuture *f =
    fdb_transaction_get_range(inflight->base.transaction,
			      start, len, 1, 0,
			      stop, len, 1, 0,
			      0, 0,
			      FDB_STREAMING_MODE_WANT_ALL, 0,
			      0, 0);
  
  fdb_future_set_callback(f, fdbfs_error_checker, p);
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
