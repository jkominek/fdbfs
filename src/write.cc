#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
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
#include "fdbfs_ops.h"

#ifdef LZ4_BLOCK_COMPRESSION
#include <lz4.h>
#endif

#ifdef ZSTD_BLOCK_COMPRESSION
#include <zstd.h>
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

  const uint8_t *val;
  int vallen;
  err = fdb_future_get_value(inode_fetch.get(), &present, &val, &vallen);
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

    if(inode.mode() & 06000) {
      // check for setuid/setgid and wipe them
      inode.set_mode(inode.mode() & 01777);
    }

    struct timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    update_mtime(&inode, &tv);

    auto key = pack_inode_key(inode.inode());
    // we've updated the inode appropriately.
    const int inode_size = inode.ByteSizeLong();
    uint8_t inode_buffer[inode_size];
    inode.SerializeToArray(inode_buffer, inode_size);
    fdb_transaction_set(transaction.get(),
			key.data(), key.size(),
			inode_buffer, inode_size);
  }

  // merge the edge writes into the blocks
  if(start_block_fetch) {
    const FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;
    fdb_error_t err;
    err = fdb_future_get_keyvalue_array(start_block_fetch.get(), &kvs, &kvcount, &more);
    if(err) return InflightAction::FDBError(err);

    const uint64_t copy_start_off = off % BLOCKSIZE;
    const uint64_t copy_start_size = std::min(buffer.size(),
                                              BLOCKSIZE - copy_start_off);
    // we don't know whats in the existing block, so we've got to plan on
    // it being full.
    const uint64_t total_buffer_size = BLOCKSIZE;
    uint8_t output_buffer[total_buffer_size];
    bzero(output_buffer, total_buffer_size);
    if(kvcount>0) {
      int ret = decode_block(&kvs[0], 0, output_buffer, total_buffer_size, total_buffer_size);
      if(ret < 0) {
        // error condition
        return InflightAction::Abort(EIO);
      }
    }
    bcopy(buffer.data(), output_buffer + copy_start_off, copy_start_size);
    auto key = pack_fileblock_key(ino, off / BLOCKSIZE);
    set_block(transaction.get(), key,
	      output_buffer, total_buffer_size, false);
  }

  if(stop_block_fetch) {
    const FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;
    fdb_error_t err;

    err = fdb_future_get_keyvalue_array(stop_block_fetch.get(), &kvs, &kvcount, &more);
    if(err) return InflightAction::FDBError(err);

    const uint64_t copysize = ((off + buffer.size()) % BLOCKSIZE);
    const uint64_t bufcopystart = buffer.size() - copysize;
    const uint64_t total_buffer_size = BLOCKSIZE;
    uint8_t output_buffer[total_buffer_size];
    bzero(output_buffer, total_buffer_size);
    if(kvcount>0) {
      int ret = decode_block(&kvs[0], 0, output_buffer, total_buffer_size, total_buffer_size);
      if(ret < 0) {
        // error condition
        return InflightAction::Abort(EIO);
      }
    }
    bcopy(buffer.data() + bufcopystart, output_buffer, copysize);
    auto key = pack_fileblock_key(ino, (off + buffer.size()) / BLOCKSIZE);
    // we don't need to preserve whatever is in the output_buffer
    // past the end of the file.
    // TODO but this is totally wrong.
    //uint64_t actual_block_size = std::min(total_buffer_size,
    //					  inode.size() % BLOCKSIZE);
    set_block(transaction.get(), key,
	      output_buffer, total_buffer_size, false);
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

  const auto conflict_start_key = pack_fileblock_key(ino, off / BLOCKSIZE);
  auto conflict_stop_key = pack_fileblock_key(ino, (off + buffer.size()) / BLOCKSIZE);
  // go past the end of the useful stop fileblock's subspace
  conflict_stop_key.push_back('\xff');
  // we're generating just a single conflict range for all of
  // the fileblocks that we're writing to: less to send across
  // the network, and for the resolver to process.
  if(fdb_transaction_add_conflict_range(transaction.get(),
					conflict_start_key.data(),
					conflict_start_key.size(),
					conflict_stop_key.data(),
					conflict_stop_key.size(),
					FDB_CONFLICT_RANGE_TYPE_WRITE)) {
    // hm, if we can't add our conflict range, we can't guarantee correctness.
    // guess we'll try again?
    return []() {
	     return InflightAction::Restart();
	   };
  }

  int iter_start, iter_stop;
  int doing_start_block = 0;
  // now, are we doing block-partial writes?
  if((off % BLOCKSIZE) != 0) {
    const int start_block = off / BLOCKSIZE;
    const auto start_key = pack_fileblock_key(ino, start_block);
    auto stop_key  = start_key;
    stop_key.push_back(0xff);
    wait_on_future(fdb_transaction_get_range(transaction.get(),
					     FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start_key.data(), start_key.size()),
					     FDB_KEYSEL_FIRST_GREATER_THAN(stop_key.data(), stop_key.size()),
					     1, 0,
					     FDB_STREAMING_MODE_WANT_ALL, 0,
					     0, 0),
		   &start_block_fetch);
    iter_start = start_block + 1;
    doing_start_block = 1;
  } else {
    iter_start = off / BLOCKSIZE;
  }

  if(((off + buffer.size()) % BLOCKSIZE) != 0) {
    const int stop_block = (off + buffer.size()) / BLOCKSIZE;
    // if the block is identical to the start block, there's no
    // sense fetching and processing it twice.
    if((!doing_start_block) || (stop_block != (off / BLOCKSIZE))) {
      const auto start_key = pack_fileblock_key(ino, stop_block);
      auto stop_key = start_key;
      stop_key.push_back(0xff);
      wait_on_future(fdb_transaction_get_range(transaction.get(),
					       FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(start_key.data(), start_key.size()),
					       FDB_KEYSEL_FIRST_GREATER_THAN(stop_key.data(), stop_key.size()),
					       1, 0,
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
    const range_keys r = offset_size_to_range_keys(ino, off, buffer.size());
    fdb_transaction_clear_range(transaction.get(),
				r.first.data(), r.first.size(),
				r.second.data(), r.second.size());
  }

  // now while those block requests are coming back to us, we can
  // process the whole blocks in the middle of the write, that don't
  // require a read-write cycle.
  for(int mid_block=iter_start; mid_block<iter_stop; mid_block++) {
    const auto key = pack_fileblock_key(ino, mid_block);
    uint8_t *block;
    block = buffer.data() + (off % BLOCKSIZE) + (mid_block - iter_start) * BLOCKSIZE;
    set_block(transaction.get(), key, block, BLOCKSIZE, false);
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
