
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

#include "util.h"
#include "inflight.h"

/*************************************************************
 * readlink
 *************************************************************
 * INITIAL PLAN
 * this should be a single get.
 *
 * REAL PLAN
 * ???
 */
class Inflight_readlink : Inflight {
public:
  Inflight_readlink(fuse_req_t, fuse_ino_t, FDBTransaction * = 0);
  Inflight_readlink *reincarnate();
  void issue();
private:
  fuse_ino_t ino;
  unique_future inode_fetch;
  void callback();
};

Inflight_readlink::Inflight_readlink(fuse_req_t req, fuse_ino_t ino,
				     FDBTransaction *transaction)
  : Inflight(req, false, transaction), ino(ino)
{
}

Inflight_readlink *Inflight_readlink::reincarnate()
{
  Inflight_readlink *x = new Inflight_readlink(req, ino,
					       transaction.release());
  delete this;
  return x;
}

void Inflight_readlink::callback()
{
  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  if(fdb_future_get_value(inode_fetch.get(),
			  &present, (const uint8_t **)&val, &vallen)) {
    restart();
    return;
  }

  if(present) {
    INodeRecord inode;
    inode.ParseFromArray(val, vallen);
    if(!inode.has_symlink()) {
      if(inode.type() == symlink) {
	abort(EIO);
      } else {
	abort(EINVAL);
      }
      return;
    }
    reply_readlink(inode.symlink().c_str());
  } else {
    abort(ENOENT);
    return;
  }
}

void Inflight_readlink::issue()
{
  auto key = pack_inode_key(ino);

  // and request just that inode
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &inode_fetch);
  cb.emplace(std::bind(&Inflight_readlink::callback, this));
  begin_wait();
}

extern "C" void fdbfs_readlink(fuse_req_t req, fuse_ino_t ino)
{
  Inflight_readlink *inflight = new Inflight_readlink(req, ino);
  inflight->issue();
}
