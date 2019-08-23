
#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 610
#include <foundationdb/fdb_c.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>

#include <algorithm>

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

class Inflight_read : Inflight {
public:
  Inflight_read(fuse_req_t, fuse_ino_t, size_t, off_t,
		FDBTransaction * = 0);
  void issue();
  Inflight_read *reincarnate();
  
private:
  unique_future inode_fetch;
  unique_future range_fetch;
  void callback();
  
  fuse_ino_t ino;
  size_t size; //size of the read
  off_t off;   //offset into file
};

Inflight_read::Inflight_read(fuse_req_t req, fuse_ino_t ino,
			     size_t size, off_t off,
			     FDBTransaction *transaction)
  : Inflight(req, false, transaction), ino(ino), size(size), off(off)
{
}

Inflight_read *Inflight_read::reincarnate()
{
  Inflight_read *x = new Inflight_read(req, ino, size, off,
				       transaction.release());
  delete this;
  return x;
}

void Inflight_read::callback()
{
  // TODO deal with the possibility of not getting all of the KV pairs
  // that we want. (that is, blocks that we want exist, but aren't sent.)

  FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;

  if(fdb_future_get_keyvalue_array(range_fetch.get(),
				   (const FDBKeyValue **)&kvs, &kvcount, &more)) {
    restart();
    return;
  }

  char buffer[size];
  bzero(buffer, size);
  int actual_bytes_received=0;

  debug_print("fdbfs_read_callback running, got %i records\n", kvcount);
  for(int i=0; i<kvcount; i++) {
    debug_print("read(%i)ing from a kv!\n", 2);
    FDBKeyValue kv = kvs[i];
    uint64_t block;
    bcopy(((uint8_t*)kv.key) + fileblock_prefix_length,
	  &block,
	  sizeof(uint64_t));
    block = be64toh(block);
    // TODO variable block size
    int buffer_last_byte;
    if( (block * BLOCKSIZE) <= off ) {
      // we need an offset into the received block, since it
      // starts before (or at) the requested read area
      uint64_t block_off = off - block * BLOCKSIZE;
      size_t copysize = std::min(size, kv.value_length - block_off);
      if(copysize>0) {
	bcopy(static_cast<const uint8_t*>(kv.value) + block_off,
	      buffer, copysize);
	buffer_last_byte = copysize;
      }
    } else {
      // we need an offset into the target buffer, as our block
      // starts after the requested read area
      int bufferoff = block * BLOCKSIZE - off;
      size_t maxcopy = size - bufferoff;
      size_t copysize = std::min(maxcopy,
				 static_cast<size_t>(kv.value_length));
      bcopy(kv.value, buffer + bufferoff, copysize);
      // this is the position of the last useful byte we wrote.
      buffer_last_byte = bufferoff + copysize;
    }

    // furthest byte in the buffer that we write a 'real' value into.
    actual_bytes_received = std::max(actual_bytes_received,
				     buffer_last_byte);
  }

  reply_buf(buffer, size);
}

void Inflight_read::issue()
{
  uint64_t start_block = htobe64(off >> BLOCKBITS);
  uint64_t stop_block  = htobe64(((off + size) >> BLOCKBITS) + 0);

  auto start = pack_fileblock_key(ino, start_block);
  auto stop  = pack_fileblock_key(ino, stop_block);

  FDBFuture *f =
    fdb_transaction_get_range(transaction.get(),
			      FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start.data(), start.size()),
			      FDB_KEYSEL_FIRST_GREATER_THAN(stop.data(), stop.size()),
			      0, 0,
			      FDB_STREAMING_MODE_WANT_ALL, 0,
			      0, 0);
  wait_on_future(f, &range_fetch);
  cb.emplace(std::bind(&Inflight_read::callback, this));
  begin_wait();
}

extern "C" void fdbfs_read(fuse_req_t req, fuse_ino_t ino, size_t size,
			   off_t off, struct fuse_file_info *fi)
{
  // given inode, figure out the appropriate key range, and
  // start reading it, filling it into a buffer to be sent back
  // with fuse_reply_buf
  Inflight_read *inflight =
    new Inflight_read(req, ino, size, off);
  inflight->issue();
}
