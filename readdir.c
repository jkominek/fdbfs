
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
 * readdir
 *************************************************************
 * INITIAL PLAN
 * ?
 *
 * REAL PLAN
 * ?
 */

struct fdbfs_inflight_readdir {
  struct fdbfs_inflight_base base;
  uint16_t dirent_prefix_len;
  fuse_ino_t ino;
  size_t size;
  off_t off;
  char *buf;
};

void fdbfs_readdir_callback(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_readdir *inflight = p;

  FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  
  fdb_future_get_keyvalue_array(f, (const FDBKeyValue **)&kvs, &kvcount, &more);

  size_t consumed_buffer = 0;
  size_t remaining_buffer = inflight->size;

  debug_print("fdfs_readdir_callback processing response for req %p. got %i KVs\n", inflight->base.req, kvcount);
  
  for(int i=0; i<kvcount; i++) {
    FDBKeyValue kv = kvs[i];

    char name[1024];
    if(kv.key_length <= inflight->dirent_prefix_len) {
      // serious internal error. we somehow got back a key that was too short?
      debug_print("fdbfs_readdir_callback internal error for request %p on kv %i. key value is shorter than the prefix of a dirent!\n", inflight->base.req, i);
      fuse_reply_err(inflight->base.req, 666);
      return;
    }
    int keylen = kv.key_length - inflight->dirent_prefix_len;
    // TOOD if keylen<=0 throw internal error.
    bcopy(((uint8_t*)kv.key) + inflight->dirent_prefix_len,
	  name,
	  keylen);
    name[keylen] = '\0'; // null terminate

    fuse_ino_t ino;
    // TODO check kv.value_length to ensure it is long enough
    bcopy(kv.value, &ino, sizeof(fuse_ino_t));
    
    struct stat attr;
    attr.st_ino = ino;
    bcopy(((uint8_t*)kv.value)+sizeof(fuse_ino_t),
	  &(attr.st_mode),
	  sizeof(mode_t));
    attr.st_mode &= S_IFMT;
    
    size_t used = fuse_add_direntry(inflight->base.req,
				    inflight->buf + consumed_buffer,
				    remaining_buffer,
				    name,
				    &attr,
				    inflight->off + i + 1);
    if(used > remaining_buffer) {
      // ran out of space. last one failed. we're done.
      break;
    }

    consumed_buffer += used;
    remaining_buffer -= used;
  }

  fuse_reply_buf(inflight->base.req, inflight->buf, consumed_buffer);
  fdb_future_destroy(f);
  fdbfs_inflight_cleanup(p);
}

void fdbfs_readdir_issuer(void *p)
{
  struct fdbfs_inflight_readdir *inflight = p;

  uint8_t start[512], stop[512];
  int len;
  pack_inode_key(inflight->ino, start, &len);
  len += 2;
  start[len-2] = 'd';
  start[len-1] = 0x00;
  bcopy(start, stop, len-1);
  stop[len-1] = 0xff;

  inflight->dirent_prefix_len = len-1;

  int offset = inflight->off;
  int limit = 10; // we should try to guess this better

#ifdef DEBUG
  char buffer1[2048], buffer2[2048];
  for(int i=0; i<len; i++) {
    sprintf(buffer1+(i<<1), "%02x", start[i]);
    sprintf(buffer2+(i<<1), "%02x", stop[i]);
  }
  debug_print("fdbfs_readdir_issuer issuing for req %p start key %s stop key %s\n",
	      inflight->base.req, buffer1, buffer2);
#endif
  
  // well this is tricky. how large a range should we request?
  FDBFuture *f =
    fdb_transaction_get_range(inflight->base.transaction,
			      start, len, 0, 1+offset,
			      stop, len, 0, 1,
			      limit, 0,
			      FDB_STREAMING_MODE_WANT_ALL, 0,
			      0, 0);

  fdb_future_set_callback(f, fdbfs_error_checker, p);
}

void fdbfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                   off_t off, struct fuse_file_info *fi)
{
  struct fdbfs_inflight_readdir *inflight;

  // fuse will know we're out of entries because we'll return 0
  // entries to a call here. but there's a decent chance we'll
  // find out on the previous call that there wasn't anything left.
  // could we fast path that case?
  // perhaps fdbfs_readdir_callback when it sees it has reached
  // the end, could set the final offset to all-1s, and we could
  // detect that here and immediately return an empty result?
  // or we could maintain a cache of how many entries we last
  // saw in a given directory. that'd let us do a better job of
  // fetching them, and if the cache entry is recent enough, and
  // we're being asked to read past the end, we could maybe bail
  // early, here.
  
  debug_print("readdir(%p, %li, %li, %li)\n",
	      req, ino, size, off);
  
  // let's not read much more than 64k in a go.
  size = min(size, 1<<16);
  
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_readdir)+size,
				   req,
				   fdbfs_readdir_callback,
				   fdbfs_readdir_issuer);
  inflight->ino = ino;
  inflight->size = size;
  inflight->off = off;
  inflight->buf = ((char*)inflight) + sizeof(struct fdbfs_inflight_readdir);

  fdbfs_readdir_issuer(inflight);
}
