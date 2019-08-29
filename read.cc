
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

class Inflight_read : public Inflight {
public:
  Inflight_read(fuse_req_t, fuse_ino_t, size_t, off_t, unique_transaction);
  InflightCallback issue();
  Inflight_read *reincarnate();
  
private:
  unique_future inode_fetch;
  unique_future range_fetch;
  InflightAction callback();
  
  fuse_ino_t ino;
  size_t requested_size; //size of the read
  off_t off;   //offset into file
};

Inflight_read::Inflight_read(fuse_req_t req, fuse_ino_t ino,
			     size_t size, off_t off,
			     unique_transaction transaction)
  : Inflight(req, false, std::move(transaction)),
    ino(ino), requested_size(size), off(off)
{
}

Inflight_read *Inflight_read::reincarnate()
{
  Inflight_read *x = new Inflight_read(req, ino, requested_size, off,
				       std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_read::callback()
{
  const uint8_t *val;
  int vallen;
  fdb_bool_t present;
  if(fdb_future_get_value(inode_fetch.get(), &present,
			  &val, &vallen)) {
    return InflightAction::Restart();
  }
  if(!present) {
    return InflightAction::Abort(EBADF);
  }
  INodeRecord inode;
  inode.ParseFromArray(val, vallen);
  if(!inode.IsInitialized()) {
    return InflightAction::Abort(EIO);
  }

  FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  if(fdb_future_get_keyvalue_array(range_fetch.get(),
				   (const FDBKeyValue **)&kvs, &kvcount, &more)) {
    return InflightAction::Restart();
  }

  if(more) {
    // TODO deal with the possibility of not getting all of the KV pairs
    // that we want. (that is, blocks that we want exist, but aren't sent.)

    // pretty straightforward, really. we move the buffer into the inflight
    // object, and then if there's more to fetch, send that request while
    // we're processing the data we already have. at the end, go back to
    // waiting instead of return.
  }

  size_t size = std::min(requested_size, inode.size() - off);

  std::vector<uint8_t> buffer(size);
  std::fill(buffer.begin(), buffer.end(), 0);
  int actual_bytes_received=0;

  for(int i=0; i<kvcount; i++) {
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
	      buffer.data(),
	      copysize);
	buffer_last_byte = copysize;
      }
    } else {
      // we need an offset into the target buffer, as our block
      // starts after the requested read area
      size_t bufferoff = block * BLOCKSIZE - off;
      // the most we can copy is the smaller of however much
      // space is left, and the size of the buffer from fdb
      size_t maxcopy = size - bufferoff;
      size_t copysize = std::min(maxcopy,
				 static_cast<size_t>(kv.value_length));
      bcopy(kv.value, buffer.data() + bufferoff, copysize);
      // this is the position of the last useful byte we wrote.
      buffer_last_byte = bufferoff + copysize;
    }

    // furthest byte in the buffer that we write a 'real' value into.
    actual_bytes_received = std::max(actual_bytes_received,
				     buffer_last_byte);
  }

  if(more) {
    // TODO see above TODO. for now, die, since we won't otherwise
    // satisfy the contract.
    return InflightAction::Abort(EIO);
  } else {
    return InflightAction::Buf(buffer);
  }
}

InflightCallback Inflight_read::issue()
{
  // we need to know how large the file is, so as to not read off the end.
  auto key = pack_inode_key(ino);
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &inode_fetch);
  
  uint64_t start_block = htobe64(off >> BLOCKBITS);
  // reading off the end of what's there doesn't matter.
  uint64_t stop_block  = htobe64(((off + requested_size) >> BLOCKBITS) + 0);

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
  return std::bind(&Inflight_read::callback, this);
}

extern "C" void fdbfs_read(fuse_req_t req, fuse_ino_t ino, size_t size,
			   off_t off, struct fuse_file_info *fi)
{
  // given inode, figure out the appropriate key range, and
  // start reading it, filling it into a buffer to be sent back
  // with fuse_reply_buf
  Inflight_read *inflight =
    new Inflight_read(req, ino, size, off, make_transaction());
  inflight->start();
}
