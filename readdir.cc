
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
#include "values.pb.h"

/*************************************************************
 * readdir
 *************************************************************
 * INITIAL PLAN
 * ?
 *
 * REAL PLAN
 * ?
 */

class Inflight_readdir : public Inflight {
public:
  Inflight_readdir(fuse_req_t, fuse_ino_t, size_t, off_t,
		   FDBTransaction * = NULL);
  InflightCallback issue();
  Inflight_readdir *reincarnate();
private:
  fuse_ino_t ino;
  size_t size;
  off_t off;
  int dirent_prefix_len;

  unique_future range_fetch;
  InflightAction callback();
};

Inflight_readdir::Inflight_readdir(fuse_req_t req, fuse_ino_t ino,
				   size_t size, off_t off,
				   FDBTransaction *transaction)
  : Inflight(req, false, transaction), ino(ino), size(size), off(off)
{
}

Inflight_readdir *Inflight_readdir::reincarnate()
{
  Inflight_readdir *x = new Inflight_readdir(req, ino, size, off, transaction.release());
  delete this;
  return x;
}

InflightAction Inflight_readdir::callback()
{
  FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  
  if(fdb_future_get_keyvalue_array(range_fetch.get(),
				   (const FDBKeyValue **)&kvs, &kvcount,
				   &more)) {
    return InflightAction::Restart();
  }

  std::vector<uint8_t> buf(size);
  size_t consumed_buffer = 0;
  size_t remaining_buffer = size;

  for(int i=0; i<kvcount; i++) {
    FDBKeyValue kv = kvs[i];

    char name[1024];
    if(kv.key_length <= dirent_prefix_len) {
      // serious internal error. we somehow got back a key that was too short?
      printf("eio!\n");
      return InflightAction::Abort(EIO);
    }
    int keylen = kv.key_length - dirent_prefix_len;
    // TOOD if keylen<=0 throw internal error.
    bcopy(((uint8_t*)kv.key) + dirent_prefix_len,
	  name,
	  keylen);
    name[keylen] = '\0'; // null terminate

    struct stat attr;
    {
      DirectoryEntry dirent;
      dirent.ParseFromArray(kv.value, kv.value_length);

      if(!dirent.IsInitialized()) {
	printf("eio!\n");
	return InflightAction::Abort(EIO);
      }
      attr.st_ino = dirent.inode();
      attr.st_mode = dirent.type();
    }
    
    size_t used = fuse_add_direntry(req,
				    reinterpret_cast<char *>(buf.data() + consumed_buffer),
				    remaining_buffer,
				    name,
				    &attr,
				    off + i + 1);
    if(used > remaining_buffer) {
      // ran out of space. last one failed. we're done.
      break;
    }

    consumed_buffer += used;
    remaining_buffer -= used;
  }

  buf.resize(consumed_buffer);
  return InflightAction::Buf(buf);
}

InflightCallback Inflight_readdir::issue()
{
  auto start = pack_inode_key(ino);
  start.push_back(DENTRY_PREFIX);
  auto stop(start);

  // this could really just be computed once, ever
  dirent_prefix_len = start.size();

  start.push_back('\x00');
  stop.push_back('\xFF');

  int offset = off;
  int limit = 10; // we should try to guess this better

  // well this is tricky. how large a range should we request?
  FDBFuture *f =
    fdb_transaction_get_range(transaction.get(),
			      start.data(), start.size(), 0, 1+offset,
			      stop.data(), stop.size(), 0, 1,
			      limit, 0,
			      FDB_STREAMING_MODE_WANT_ALL, 0,
			      0, 0);
  wait_on_future(f, &range_fetch);
  return std::bind(&Inflight_readdir::callback, this);
}

extern "C" void fdbfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
			      off_t off, struct fuse_file_info *fi)
{
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
  
  // let's not read much more than 64k in a go.
  Inflight_readdir *inflight =
    new Inflight_readdir(req, ino, std::min(size, static_cast<size_t>(1<<16)), off);

  inflight->start();
}
