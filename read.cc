
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
#include "fdbfs_ops.h"

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
  fdb_error_t err;
  err = fdb_future_get_value(inode_fetch.get(), &present, &val, &vallen);
  if(err)
    return InflightAction::FDBError(err);
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
  err = fdb_future_get_keyvalue_array(range_fetch.get(), (const FDBKeyValue **)&kvs, &kvcount, &more);
  if(err)
    return InflightAction::FDBError(err);

  if(more) {
    // TODO deal with the possibility of not getting all of the KV pairs
    // that we want. (that is, blocks that we want exist, but aren't sent.)

    // pretty straightforward, really. we move the buffer into the inflight
    // object, and then if there's more to fetch, send that request while
    // we're processing the data we already have. at the end, go back to
    // waiting instead of return.
  }

  size_t size = std::min(requested_size, inode.size() - off);

  // we pad the buffer some so special block decoders have room to work
  // without having to perform extra copies or allocations.
  std::vector<uint8_t> buffer(size + 32);
  std::fill(buffer.begin(), buffer.end(), 0);

  for(int i=0; i<kvcount; i++) {
    FDBKeyValue kv = kvs[i];
    std::vector<uint8_t> key(kv.key_length);
    bcopy(kv.key, key.data(), kv.key_length);
    print_key(key);
    uint64_t block;
    bcopy(((uint8_t*)kv.key) + fileblock_prefix_length,
	  &block,
	  sizeof(uint64_t));
    block = be64toh(block);
    // TODO variable block size
    if( (block * BLOCKSIZE) <= off ) {
      // we need an offset into the received block, since it
      // starts before (or at) the requested read area
      uint64_t block_off = off - block * BLOCKSIZE;
      int d = decode_block(&kv, block_off, buffer.data(), size, buffer.size());
      if(d<0)
	return InflightAction::Abort(EIO);
    } else {
      // we need an offset into the target buffer, as our block
      // starts after the requested read area.
      size_t bufferoff = block * BLOCKSIZE - off;
      int d = decode_block(&kv, 0, buffer.data() + bufferoff, size - bufferoff, buffer.size() - bufferoff);
      if(d<0)
	return InflightAction::Abort(EIO);
    }
  }

  if(more) {
    // TODO see above TODO. for now, die, since we won't otherwise
    // satisfy the contract.
    return InflightAction::Abort(EIO);
  } else {
    return InflightAction::Buf(buffer, size);
  }
}

InflightCallback Inflight_read::issue()
{
  // we need to know how large the file is, so as to not read off the end.
  auto key = pack_inode_key(ino);
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &inode_fetch);
  
  range_keys r = offset_size_to_range_keys(ino, off, requested_size);

  FDBFuture *f =
    fdb_transaction_get_range(transaction.get(),
			      FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(r.first.data(), r.first.size()),
			      FDB_KEYSEL_FIRST_GREATER_THAN(r.second.data(), r.second.size()),
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
