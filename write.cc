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

#ifdef LZ4_BLOCK_COMPRESSION
#include <lz4.h>
#endif

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
 * PLAN SO FAR
 * 1) On issuance, we'll always request the inode record
 * 2) If we're asked to perform partial writes to a block (or blocks)
 *    we'll fetch those blocks, so that we can merge the new contents
 *    in with the existing.
 * 3) while we're waiting, load any whole-block writes into the
 *    FDB transaction (an entirely client-side operation)
 * 4) once we've received all of the relevant futures, check the
 *    inode, and merge partial writes into the relevant blocks.
 * 5) commit
 *
 * REAL PLAN?
 * ???
 */

inline void sparsify(uint8_t *block, uint64_t *write_size) {
  // sparsify our writes, by truncating nulls from the end of
  // blocks, and just clearing away totally null blocks 
  for(; *(write_size)>0; *(write_size)-=1) {
    if(block[*(write_size)-1] != 0x00)
      break;
  }
}

void set_block(FDBTransaction *transaction, std::vector<uint8_t> key,
	       uint8_t *buffer, uint64_t size)
{
  sparsify(buffer, &size);
  if(size>0) {
    // TODO here's where we'd implement the write-side cleverness for our
    // block encoding schemes. they should all not only be ifdef'd, but
    // check for whether or not the feature is enabled on the filesystem.
#ifdef BLOCK_COMPRESSION
    if(size>=64) {
      // considering that these blocks may be stored 3 times, and over
      // their life may have to be moved repeatedly across WANs between
      // data centers, we'll accept very small amounts of compression:
      int acceptable_size = BLOCKSIZE - 16;
      uint8_t compressed[BLOCKSIZE];
      // we're arbitrarily saying blocks should be at least 64 bytes
      // after sparsification, before we'll attempt to compress them.
#ifdef LZ4_BLOCK_COMPRESSION
      int ret = LZ4_compress_default(reinterpret_cast<char*>(buffer),
				     reinterpret_cast<char*>(compressed),
				     size, BLOCKSIZE);
      if((0<ret) && (ret <= acceptable_size)) {
	  // ok, we'll take it.
	  key.push_back('z'); // compressed
	  key.push_back(0x01); // 1 byte of arguments
	  key.push_back(0x00); // LZ4 marker
	  fdb_transaction_set(transaction, key.data(), key.size(),
			      compressed, ret);
	  return;
      }
#endif
    }
#endif
    // we'll fall back to this if none of the compression schemes bail out
    fdb_transaction_set(transaction, key.data(), key.size(), buffer, size);
  } else {
    // storage model allows for sparsity; interprets missing blocks as nulls
  }
}

class Inflight_write : public Inflight {
public:
  Inflight_write(fuse_req_t, fuse_ino_t, std::vector<uint8_t>, off_t,
		 unique_transaction);
  Inflight_write *reincarnate();
  InflightCallback issue();
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

  InflightAction check();
  InflightAction commit_cb();
  unique_future commit;
};

Inflight_write::Inflight_write(fuse_req_t req, fuse_ino_t ino,
			       std::vector<uint8_t> buffer, off_t off,
			       unique_transaction transaction)
  : Inflight(req, true, std::move(transaction)),
    ino(ino), buffer(buffer), off(off)
{
}

Inflight_write *Inflight_write::reincarnate()
{
  Inflight_write *x =
    new Inflight_write(req, ino, buffer, off, std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_write::commit_cb()
{
  return InflightAction::Write(buffer.size());
}

InflightAction Inflight_write::check()
{
  fdb_bool_t present;
  fdb_error_t err;

  uint8_t *val;
  int vallen;
  err = fdb_future_get_value(inode_fetch.get(), &present, (const uint8_t **)&val, &vallen);
  if(err) return InflightAction::FDBError(err);
  // check everything about the inode
  if(!present)
  {
    // this inode doesn't exist.
    return InflightAction::Abort(EBADF);
  }

  INodeRecord inode;
  inode.ParseFromArray(val, vallen);
  if((!inode.IsInitialized()) ||
     (!inode.has_type()) || (!inode.has_size()) ||
     (inode.type() != regular)) {
    return InflightAction::Abort(EINVAL);
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

  // merge the edge writes into the blocks
  if(start_block_fetch) {
    FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;
    fdb_error_t err;
    err = fdb_future_get_keyvalue_array(start_block_fetch.get(), (const FDBKeyValue **)&kvs, &kvcount, &more);
    if(err) return InflightAction::FDBError(err);

    uint64_t copy_start_off = off % BLOCKSIZE;
    uint64_t copy_start_size = std::min(buffer.size(),
					BLOCKSIZE - copy_start_off);
    uint64_t total_buffer_size = copy_start_off + copy_start_size;
    uint8_t output_buffer[total_buffer_size];
    bzero(output_buffer, total_buffer_size);
    if(kvcount>0) {
      // TODO check for error
      decode_block(&kvs[0], 0, output_buffer, copy_start_off, total_buffer_size);
    }
    bcopy(buffer.data(), output_buffer + copy_start_off, copy_start_size);
    auto key = pack_fileblock_key(ino, off / BLOCKSIZE);
    set_block(transaction.get(), key,
	      output_buffer, total_buffer_size);
  }

  if(stop_block_fetch) {
    FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;
    fdb_error_t err;

    err = fdb_future_get_keyvalue_array(stop_block_fetch.get(), (const FDBKeyValue **)&kvs, &kvcount, &more);
    if(err) return InflightAction::FDBError(err);

    uint64_t copysize = ((off + buffer.size()) % BLOCKSIZE);
    uint64_t bufcopystart = buffer.size() - copysize;
    uint64_t total_buffer_size = BLOCKSIZE;
    uint8_t output_buffer[total_buffer_size];
    bzero(output_buffer, total_buffer_size);
    if(kvcount>0) {
      // TODO check for error
      decode_block(&kvs[0], 0, output_buffer, total_buffer_size, total_buffer_size);
    }
    bcopy(buffer.data() + bufcopystart, output_buffer, copysize);
    auto key = pack_fileblock_key(ino, (off + buffer.size()) / BLOCKSIZE);
    // we don't need to consider whatever is in the output_buffer
    // past the end of the file.
    uint64_t actual_block_size = std::min(total_buffer_size,
					  inode.size() % BLOCKSIZE);
    set_block(transaction.get(), key,
	      output_buffer, actual_block_size);
  }

  // perform all of the writes
  wait_on_future(fdb_transaction_commit(transaction.get()),
		 &commit);

  return InflightAction::BeginWait(std::bind(&Inflight_write::commit_cb, this));
}

InflightCallback Inflight_write::issue()
{
  // turn off RYW, so there's no uncertainty about what we'll get when
  // we interleave our reads and writes.
  if(fdb_transaction_set_option(transaction.get(),
				FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE,
				NULL, 0)) {
    // hmm.
    // TODO how do we generate an error here?
    return []() {
      // i don't think this will be run, since we've registered no futures?
      return InflightAction::Abort(EIO);
    };
  }

  // step 1 is easy, we'll need the inode record
  {
    auto key = pack_inode_key(ino);
    wait_on_future(fdb_transaction_get(transaction.get(),
				       key.data(), key.size(), 0),
		   &inode_fetch);
  }

  int iter_start, iter_stop;
  int doing_start_block = 0;
  // now, are we doing block-partial writes?
  if((off % BLOCKSIZE) != 0) {
    int start_block = off / BLOCKSIZE;
    auto start_key = pack_fileblock_key(ino, start_block);
    auto stop_key  = pack_fileblock_key(ino, start_block+1);
    wait_on_future(fdb_transaction_get_range(transaction.get(),
					     FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start_key.data(), start_key.size()),
					     FDB_KEYSEL_FIRST_GREATER_THAN(stop_key.data(), stop_key.size()),
					     0, 0,
					     FDB_STREAMING_MODE_WANT_ALL, 0,
					     0, 0),
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
      auto start_key = pack_fileblock_key(ino, stop_block);
      auto stop_key = pack_fileblock_key(ino, stop_block+1);
      wait_on_future(fdb_transaction_get_range(transaction.get(),
					       FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start_key.data(), start_key.size()),
					       FDB_KEYSEL_FIRST_GREATER_THAN(stop_key.data(), stop_key.size()),
					       0, 0,
					       FDB_STREAMING_MODE_WANT_ALL, 0,
					       0, 0),
		     &stop_block_fetch);
    }
    iter_stop = stop_block;
  } else {
    iter_stop = (off + buffer.size()) / BLOCKSIZE;
  }

  {
    // Clear all of the blocks we might be replacing, since we're
    // not sure if they're plain, compressed, or some other variant
    // We're doing this in the spot where it ought to be safe even
    // if RYW is turned on.
    range_keys r = offset_size_to_range_keys(ino, off, buffer.size());
    fdb_transaction_clear_range(transaction.get(),
				r.first.data(), r.first.size(),
				r.second.data(), r.second.size());
  }

  // now while those block requests are coming back to us, we can
  // process the whole blocks in the middle of the write, that don't
  // require a read-write cycle.
  for(int mid_block=iter_start; mid_block<iter_stop; mid_block++) {
    auto key = pack_fileblock_key(ino, mid_block);
    uint8_t *block;
    block = buffer.data() + (off % BLOCKSIZE) + (mid_block - iter_start) * BLOCKSIZE;
    set_block(transaction.get(), key, block, BLOCKSIZE);
  }

  return std::bind(&Inflight_write::check, this);
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
    new Inflight_write(req, ino, buffer, off, make_transaction());
  inflight->start();
}
