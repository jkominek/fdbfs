
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
struct fdbfs_inflight_readlink {
  struct fdbfs_inflight_base base;
  fuse_ino_t ino;
};

void fdbfs_readlink_callback(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_readlink *inflight = p;

  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  fdb_future_get_value(f, &present, (const uint8_t **)&val, &vallen);

  if(present) {
    INodeRecord *inode = inode_record__unpack(NULL, vallen, val);
    if((inode==NULL) || (inode->symlink==NULL)) {
      // no good
    }
    fuse_reply_readlink(inflight->base.req, inode->symlink);
    inode_record__free_unpacked(inode, NULL);
  } else {
    fuse_reply_err(inflight->base.req, ENOENT);
  }

  fdb_future_destroy(f);
  fdbfs_inflight_cleanup(p);
}

void fdbfs_readlink_issuer(void *p)
{
  struct fdbfs_inflight_readlink *inflight = p;

  // pack the inode key
  uint8_t key[512];
  int keylen;

  pack_inode_key(inflight->ino, key, &keylen);

  // and request just that inode
  FDBFuture *f = fdb_transaction_get(inflight->base.transaction, key, keylen, 0);

  fdb_future_set_callback(f, fdbfs_error_checker, p);
}

void fdbfs_readlink(fuse_req_t req, fuse_ino_t ino)
{
  // get the file attributes of an inode
  struct fdbfs_inflight_readlink *inflight;
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_readlink),
				   req,
				   fdbfs_readlink_callback,
				   fdbfs_readlink_issuer,
				   T_READONLY);
  inflight->ino = ino;

  fdbfs_readlink_issuer(inflight);
}
