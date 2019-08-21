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
#include <sys/time.h>

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

struct fdbfs_inflight_write {
  struct fdbfs_inflight_base base;
  FDBFuture *inode_fetch;
  // the future getting the two blocks that can be at the ends
  // of a write, which thus have to be read in order to perform
  // this write. 0, 1 or 2 of them may be null.
  FDBFuture *start_block_fetch;
  FDBFuture *stop_block_fetch;
  fuse_ino_t ino;
  uint8_t *buf;
  size_t size;
  off_t off;
};

void fdbfs_write_commit_cb(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_write *inflight = p;
  printf("commited write for %li bytes\n", inflight->size);
  fuse_reply_write(inflight->base.req, inflight->size);
  fdb_future_destroy(f);
  fdbfs_inflight_cleanup(p);
}

void fdbfs_write_check(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_write *inflight = p;

  if(!fdb_future_is_ready(inflight->inode_fetch)) {
    fdb_future_set_callback(inflight->inode_fetch, fdbfs_error_checker, p);
    return;
  }
  if((inflight->start_block_fetch!=NULL) &&
     (!fdb_future_is_ready(inflight->start_block_fetch))) {
    fdb_future_set_callback(inflight->start_block_fetch, fdbfs_error_checker, p);
    return;
  }
  if((inflight->stop_block_fetch!=NULL) &&
     (!fdb_future_is_ready(inflight->stop_block_fetch))) {
    fdb_future_set_callback(inflight->stop_block_fetch, fdbfs_error_checker, p);
    return;
  }

  fdb_bool_t present;
  int err = 0;

  uint8_t *val;
  int vallen;
  fdb_future_get_value(inflight->inode_fetch, &present,
		       (const uint8_t **)&val, &vallen);
  // check everything about the inode
  if(present)
  {
    INodeRecord *inode = inode_record__unpack(NULL, vallen, val);

    if((inode == NULL) ||
       (!inode->has_type) || (!inode->has_size) ||
       (inode->type != FILETYPE__regular)) {
      err = EINVAL;
    } else {
      if(inode->size < (inflight->off + inflight->size)) {
	// we need to expand size of the file
	inode->size = inflight->off + inflight->size;
      }

      struct timeval tv;
      gettimeofday(&tv, NULL);

      if(inode->atime) {
	inode->atime->sec = tv.tv_sec;
	inode->atime->nsec = tv.tv_usec * 1000;
      }
      if(inode->mtime) {
	inode->mtime->sec = tv.tv_sec;
	inode->mtime->nsec = tv.tv_usec * 1000;
      }

      uint8_t key[2048];
      int keylen;
      pack_inode_key(inode->inode, key, &keylen);
      // we've updated the inode appropriately.
      int inode_size = inode_record__get_packed_size(inode);
      uint8_t inode_buffer[inode_size];
      inode_record__pack(inode, inode_buffer);
      fdb_transaction_set(inflight->base.transaction,
			  key, keylen,
			  inode_buffer, inode_size);
    }

    inode_record__free_unpacked(inode, NULL);
  } else {
    // this inode doesn't exist.
    err = EBADF;
  }
  fdb_future_destroy(inflight->inode_fetch);

  if(err) {
    fuse_reply_err(inflight->base.req, err);

    if(inflight->start_block_fetch)
      fdb_future_destroy(inflight->start_block_fetch);
    if(inflight->stop_block_fetch)
      fdb_future_destroy(inflight->stop_block_fetch);
    fdbfs_inflight_cleanup(p);
    return;
  }
  
  // merge the edge writes into the blocks
  if(inflight->start_block_fetch) {
    fdb_future_get_value(inflight->start_block_fetch, &present,
			 (const uint8_t **)&val, &vallen);

    int copy_start_off = inflight->off % BLOCKSIZE;
    int copy_start_size = min(inflight->size,
			      BLOCKSIZE - copy_start_off);
    int total_buffer_size = max(vallen,
				copy_start_off + copy_start_size);
    uint8_t buffer[total_buffer_size];
    bzero(buffer, total_buffer_size);
    bcopy(val, buffer, vallen);
    bcopy(inflight->buf, buffer + copy_start_off, copy_start_size);

    uint8_t key[2048];
    int keylen;
    pack_fileblock_key(inflight->ino, 
		       inflight->off / BLOCKSIZE,
		       key, &keylen);
    fdb_transaction_set(inflight->base.transaction,
			key, keylen,
			buffer, total_buffer_size);
    fdb_future_destroy(inflight->start_block_fetch);
  }

  if(inflight->stop_block_fetch) {
    fdb_future_get_value(inflight->stop_block_fetch, &present,
			 (const uint8_t **)&val, &vallen);
    int copysize = ((inflight->off + inflight->size) % BLOCKSIZE);
    int bufcopystart = inflight->size - copysize;
    int total_buffer_size = max(copysize, vallen);
    uint8_t buffer[total_buffer_size];
    bzero(buffer, total_buffer_size);
    bcopy(val, buffer, vallen);
    bcopy(inflight->buf + bufcopystart,
	  buffer, copysize);

    uint8_t key[2048];
    int keylen;
    pack_fileblock_key(inflight->ino, 
		       (inflight->off + inflight->size) / BLOCKSIZE,
		       key, &keylen);
    fdb_transaction_set(inflight->base.transaction,
			key, keylen,
			buffer, total_buffer_size);
    fdb_future_destroy(inflight->stop_block_fetch);
  }

  // perform all of the writes
  inflight->base.cb = fdbfs_write_commit_cb;
  fdb_future_set_callback(fdb_transaction_commit(inflight->base.transaction),
			  fdbfs_error_checker, p);
}

void fdbfs_write_issuer(void *p)
{
  struct fdbfs_inflight_write *inflight = p;

  // step 1 is easy, we'll need the inode record
  uint8_t key[512];
  int keylen;
  pack_inode_key(inflight->ino, key, &keylen);
  inflight->inode_fetch =
    fdb_transaction_get(inflight->base.transaction, key, keylen, 0);
  fdb_future_set_callback(inflight->inode_fetch,
			  fdbfs_error_checker, p);

  int iter_start, iter_stop;
  int doing_start_block = 0;
  // now, are we doing block-partial writes?
  if((inflight->off % BLOCKSIZE) != 0) {
    int start_block = inflight->off / BLOCKSIZE;
    pack_fileblock_key(inflight->ino, start_block,
		       key, &keylen);
    inflight->start_block_fetch =
      fdb_transaction_get(inflight->base.transaction,
			  key, keylen, 0);
    iter_start = start_block + 1;
    doing_start_block = 1;
    printf("partial start block\n");
  } else {
    iter_start = inflight->off / BLOCKSIZE;
    printf("no start block\n");
  }

  if(((inflight->off + inflight->size) % BLOCKSIZE) != 0) {
    int stop_block = (inflight->off + inflight->size) / BLOCKSIZE;
    // if the block is identical to the start block, there's no
    // sense fetching and processing it twice.
    if((!doing_start_block) || (stop_block != (inflight->off / BLOCKSIZE))) {
      pack_fileblock_key(inflight->ino, stop_block,
			 key, &keylen);
      inflight->stop_block_fetch =
	fdb_transaction_get(inflight->base.transaction,
			    key, keylen, 0);
    }
    iter_stop = stop_block;
    printf("partial stop block\n");
  } else {
    iter_stop = (inflight->off + inflight->size) / BLOCKSIZE;
    printf("no stop block\n");
  }

  // now while those block requests are coming back to us, we can
  // process the whole blocks in the middle of the write, that don't
  // require a read-write cycle.
  printf("iterating over middle blocks from %i to %i\n", iter_start, iter_stop);
  for(int mid_block=iter_start; mid_block<iter_stop; mid_block++) {
    pack_fileblock_key(inflight->ino, mid_block, key, &keylen);
    uint8_t *block;
    block = inflight->buf + (inflight->off % BLOCKSIZE) + mid_block * BLOCKSIZE;
    fdb_transaction_set(inflight->base.transaction,
			key, keylen,
			block, BLOCKSIZE);
  }
}

void fdbfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
		 size_t size, off_t off, struct fuse_file_info *fi)
{
  if(size==0) {
    // just in case?
    fuse_reply_write(req, 0);
    return;
  }
  
  struct fdbfs_inflight_write *inflight;
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_write) +
				   size,
				   req,
				   fdbfs_write_check,
				   fdbfs_write_issuer,
				   T_READWRITE);
  inflight->ino = ino;
  inflight->buf = ((uint8_t*)inflight) + sizeof(struct fdbfs_inflight_write);
  inflight->off = off;
  inflight->size = size;
  inflight->start_block_fetch = inflight->stop_block_fetch = NULL;
  bcopy(buf, inflight->buf, size);

  printf("fdbfs_write(%p, %lx, %c, %li, %li)\n",
	 req, ino, *buf, size, off);
  
  fdbfs_write_issuer(inflight);
}
