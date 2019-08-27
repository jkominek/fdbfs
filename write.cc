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
#include <sys/time.h>

#include <algorithm>

#include "util.h"
#include "inflight.h"

/*************************************************************
 * write
 *************************************************************
 * DOCS
 *     """Write should return exactly the number of bytes requested
 * except on error. An exception to this is when the file has been
 * opened in 'direct_io' mode, in which case the return value of the
 * write system call will reflect the return value of this operation.
 * Unless FUSE_CAP_HANDLE_KILLPRIV is disabled, this method is
 * expected to reset the setuid and setgid bits."""
 *
 * INITIAL PLAN
 * ???
 *
 * REAL PLAN?
 * in reality we'll probably also need to read the file attributes
 * and pick up the file size, in addition to the content reads.
 *
 * that still isn't too bad, but it means that we'll have to
 * handle multiple futures in the end.
 */

class Inflight_write : Inflight {
public:
  Inflight_write(fuse_req_t, fuse_ino_t, std::vector<uint8_t>, off_t,
		 FDBTransaction * = 0);
  Inflight_write *reincarnate();
  void issue();
private:
  unique_future inode_fetch;
  // the future getting the two blocks that can be at the ends
  // of a write, which thus have to be read in order to perform
  // this write. 0, 1 or 2 of them may be null.
  unique_future start_block_fetch;
  unique_future stop_block_fetch;
  fuse_ino_t ino;
  std::vector<uint8_t> buffer;
  off_t off;

  void check();
  void commit_cb();
  unique_future commit;
};

Inflight_write::Inflight_write(fuse_req_t req, fuse_ino_t ino,
			       std::vector<uint8_t> buffer, off_t off,
			       FDBTransaction *transaction)
  : Inflight(req, true, transaction), ino(ino), buffer(buffer), off(off)
{
}

Inflight_write *Inflight_write::reincarnate()
{
  Inflight_write *x =
    new Inflight_write(req, ino, buffer, off, transaction.release());
  delete this;
  return x;
}

void Inflight_write::commit_cb()
{
  reply_write(buffer.size());
}

void Inflight_write::check()
{
  fdb_bool_t present;

  uint8_t *val;
  int vallen;
  if(fdb_future_get_value(inode_fetch.get(), &present,
			  (const uint8_t **)&val, &vallen)) {
    restart();
    return;
  }
  // check everything about the inode
  if(present)
  {
    INodeRecord inode;
    inode.ParseFromArray(val, vallen);

    if((!inode.IsInitialized()) ||
       (!inode.has_type()) || (!inode.has_size()) ||
       (inode.type() != regular)) {
      abort(EINVAL);
      return;
    } else {
      if(inode.size() < (off + buffer.size())) {
	// we need to expand size of the file
	inode.set_size(off + buffer.size());
      }

      struct timespec tv;
      clock_gettime(CLOCK_REALTIME, &tv);
      update_mtime(&inode, &tv);

      auto key = pack_inode_key(inode.inode());
      // we've updated the inode appropriately.
      int inode_size = inode.ByteSize();
      uint8_t inode_buffer[inode_size];
      inode.SerializeToArray(inode_buffer, inode_size);
      fdb_transaction_set(transaction.get(),
			  key.data(), key.size(),
			  inode_buffer, inode_size);
    }
  } else {
    // this inode doesn't exist.
    abort(EBADF);
  }

  // merge the edge writes into the blocks
  if(start_block_fetch) {
    if(fdb_future_get_value(start_block_fetch.get(), &present,
			    (const uint8_t **)&val, &vallen)) {
      restart();
      return;
    }

    uint64_t copy_start_off = off % BLOCKSIZE;
    uint64_t copy_start_size = std::min(buffer.size(),
					BLOCKSIZE - copy_start_off);
    uint64_t total_buffer_size = std::max(static_cast<uint64_t>(vallen),
					  copy_start_off + copy_start_size);
    uint8_t output_buffer[total_buffer_size];
    bzero(output_buffer, total_buffer_size);
    bcopy(val, output_buffer, vallen);
    bcopy(buffer.data(), output_buffer + copy_start_off, copy_start_size);

    auto key = pack_fileblock_key(ino, off / BLOCKSIZE);
    fdb_transaction_set(transaction.get(),
			key.data(), key.size(),
			output_buffer, total_buffer_size);
  }

  if(stop_block_fetch) {
    if(fdb_future_get_value(stop_block_fetch.get(), &present,
			    (const uint8_t **)&val, &vallen)) {
      restart();
      return;
    }
    uint64_t copysize = ((off + buffer.size()) % BLOCKSIZE);
    uint64_t bufcopystart = buffer.size() - copysize;
    uint64_t total_buffer_size = std::max(copysize,
					  static_cast<uint64_t>(vallen));
    uint8_t output_buffer[total_buffer_size];
    bzero(output_buffer, total_buffer_size);
    bcopy(val, output_buffer, vallen);
    bcopy(buffer.data() + bufcopystart,
	  output_buffer, copysize);

    auto key = pack_fileblock_key(ino, (off + buffer.size()) / BLOCKSIZE);
    fdb_transaction_set(transaction.get(),
			key.data(), key.size(),
			output_buffer, total_buffer_size);
  }

  // perform all of the writes
  wait_on_future(fdb_transaction_commit(transaction.get()),
		 &commit);
  cb.emplace(std::bind(&Inflight_write::commit_cb, this));
  begin_wait();
}

void Inflight_write::issue()
{
  // step 1 is easy, we'll need the inode record
  auto key = pack_inode_key(ino);
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &inode_fetch);
  cb.emplace(std::bind(&Inflight_write::check, this));

  int iter_start, iter_stop;
  int doing_start_block = 0;
  // now, are we doing block-partial writes?
  if((off % BLOCKSIZE) != 0) {
    int start_block = off / BLOCKSIZE;
    key = pack_fileblock_key(ino, start_block);
    wait_on_future(fdb_transaction_get(transaction.get(),
				       key.data(), key.size(), 0),
		   &start_block_fetch);
    iter_start = start_block + 1;
    doing_start_block = 1;
  } else {
    iter_start = off / BLOCKSIZE;
  }

  if(((off + buffer.size()) % BLOCKSIZE) != 0) {
    int stop_block = (off + buffer.size()) / BLOCKSIZE;
    // if the block is identical to the start block, there's no
    // sense fetching and processing it twice.
    if((!doing_start_block) || (stop_block != (off / BLOCKSIZE))) {
      key = pack_fileblock_key(ino, stop_block);
      wait_on_future(fdb_transaction_get(transaction.get(),
					 key.data(), key.size(), 0),
		     &stop_block_fetch);
    }
    iter_stop = stop_block;
  } else {
    iter_stop = (off + buffer.size()) / BLOCKSIZE;
  }

  // now while those block requests are coming back to us, we can
  // process the whole blocks in the middle of the write, that don't
  // require a read-write cycle.
  for(int mid_block=iter_start; mid_block<iter_stop; mid_block++) {
    key = pack_fileblock_key(ino, mid_block);
    uint8_t *block;
    block = buffer.data() + (off % BLOCKSIZE) + mid_block * BLOCKSIZE;
    fdb_transaction_set(transaction.get(),
			key.data(), key.size(),
			block, BLOCKSIZE);
  }

  begin_wait();
}

extern "C" void fdbfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
			    size_t size, off_t off, struct fuse_file_info *fi)
{
  if(size==0) {
    // just in case?
    fuse_reply_write(req, 0);
    return;
  }

  std::vector<uint8_t> buffer(buf, buf+size);
  Inflight_write *inflight =
    new Inflight_write(req, ino, buffer, off);
  inflight->issue();
}
