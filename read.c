
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
  // how long is the portion of the key before the bytes
  // representing the offset
  uint16_t data_prefix_len;
  size_t size; //size of the read
  off_t off;   //offset into file
};

void fdbfs_read_callback(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_read *inflight = p;

  // TODO deal with the possibility of not getting all of the KV pairs
  // that we want.

  FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;

  fdb_future_get_keyvalue_array(f, (const FDBKeyValue **)&kvs, &kvcount, &more);

  char buffer[inflight->size];
  bzero(buffer, inflight->size);
  int actual_bytes_received=0;

  debug_print("fdbfs_read_callback running, got %i records\n", kvcount);
  for(int i=0; i<kvcount; i++) {
    debug_print("read(%i)ing from a kv!\n", 2);
    FDBKeyValue kv = kvs[i];
    uint64_t block;
    bcopy(((uint8_t*)kv.key) + inflight->data_prefix_len,
	  &block,
	  sizeof(uint64_t));
    block = be64toh(block);
    // TODO variable block size
    int buffer_last_byte;
    if( (block * BLOCKSIZE) <= inflight->off ) {
      // we need an offset into the received block, since it
      // starts before (or at) the requested read area
      uint64_t block_off = inflight->off - block * BLOCKSIZE;
      size_t copysize = min(inflight->size, kv.value_length - block_off);
      if(copysize>0) {
	bcopy(kv.value + block_off, buffer, copysize);
	buffer_last_byte = copysize;
      }
    } else {
      // we need an offset into the target buffer, as our block
      // starts after the requested read area
      int bufferoff = block * BLOCKSIZE - inflight->off;
      size_t maxcopy = inflight->size - bufferoff;
      size_t copysize = min(maxcopy, kv.value_length);
      bcopy(kv.value, buffer + bufferoff, copysize);
      // this is the position of the last useful byte we wrote.
      buffer_last_byte = bufferoff + copysize;
    }

    // furthest byte in the buffer that we write a 'real' value into.
    actual_bytes_received = max(actual_bytes_received,
				buffer_last_byte);
  }

  fuse_reply_buf(inflight->base.req, buffer, inflight->size);
  fdb_future_destroy(f);
  fdbfs_inflight_cleanup(p);
}

void fdbfs_read_issuer(void *p)
{
  struct fdbfs_inflight_read *inflight;
  inflight = (struct fdbfs_inflight_read *)p;

  uint8_t start[512], stop[512];
  int len;
  pack_inode_key(inflight->ino, start, &len);
  pack_inode_key(inflight->ino, stop, &len);
  start[len] = stop[len] = 'f';
  len += 1;
  // TODO variable block size
  uint64_t start_block = htobe64(inflight->off >> BLOCKBITS);
  uint64_t stop_block  = htobe64(((inflight->off + inflight->size) >> BLOCKBITS) + 0);
  bcopy(&start_block, start+len, sizeof(uint64_t));
  bcopy(&stop_block, stop+len, sizeof(uint64_t));
  inflight->data_prefix_len = len;
  len += sizeof(uint64_t);

#ifdef DEBUG
  char buffera[2048], bufferb[2048];
  for(int i=0; i<len; i++) {
    sprintf(buffera+(i<<2), "\\x%02x", start[i]);
    sprintf(bufferb+(i<<2), "\\x%02x", stop[i]);
  }
  debug_print("fdbfs_read_issuer for req %p reading from block %li (key %s) to block %li (%s)\n",
	      inflight->base.req,
	      (inflight->off >> BLOCKBITS),
	      buffera,
	      (inflight->off + inflight->size) >> BLOCKBITS,
	      bufferb);
#endif
  
  FDBFuture *f =
    fdb_transaction_get_range(inflight->base.transaction,
			      FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start,len),
			      FDB_KEYSEL_FIRST_GREATER_THAN(stop,len),
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
				   fdbfs_read_issuer,
				   T_READONLY);
  debug_print("fdbfs_read got a read for %li bytes at %li\n", size, off);
  inflight->ino = ino;
  inflight->size = size;
  inflight->off = off;

  fdbfs_read_issuer(inflight);
}
