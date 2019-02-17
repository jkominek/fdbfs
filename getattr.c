
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
 * getattr
 *************************************************************
 * INITIAL PLAN
 * this should be a single get.
 *
 * REAL PLAN
 * maybe a small range read to pick up extended attributes
 * or less common values?
 */
struct fdbfs_inflight_getattr {
  struct fdbfs_inflight_base base;
  fuse_ino_t ino;
};

void fdbfs_getattr_callback(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_getattr *inflight = p;

  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  fdb_future_get_value(f, &present, (const uint8_t **)&val, &vallen);

  if(present) {
    struct stat attr;

    unpack_stat_from_dbvalue(val, vallen, &attr);
    debug_print("getattr returning for inode %016lx\n", inflight->ino);
    fuse_reply_attr(inflight->base.req, &attr, 0.0);
  } else {
    debug_print("getattr failed to find inode %016lx\n", inflight->ino);
    fuse_reply_err(inflight->base.req, EFAULT);
  }

  fdb_future_destroy(f);
  fdbfs_inflight_cleanup(p);
}

void fdbfs_getattr_issuer(void *p)
{
  struct fdbfs_inflight_getattr *inflight = p;

  // pack the inode key
  uint8_t key[512];
  int keylen;

  pack_inode_key(inflight->ino, key, &keylen);

#ifdef DEBUG
  char printbuf[2048];
  for(int i=0; i<keylen; i++)
    sprintf(printbuf+(i<<1), "%02x", key[i]);
  
  debug_print("fdbfs_getattr_issuer issuing for req %p key %s\n", inflight->base.req, printbuf);
#endif
  
  // and request just that inode
  FDBFuture *f = fdb_transaction_get(inflight->base.transaction, key, keylen, 0);

  fdb_future_set_callback(f, fdbfs_error_checker, p);
}

void fdbfs_getattr(fuse_req_t req, fuse_ino_t ino,
		   struct fuse_file_info *fi)
{
  // get the file attributes of an inode
  struct fdbfs_inflight_getattr *inflight;
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_getattr),
				   req,
				   fdbfs_getattr_callback,
				   fdbfs_getattr_issuer);
  inflight->ino = ino;

  debug_print("fdbfs_getattr taking off for req %p\n", inflight->base.req);
  
  fdbfs_getattr_issuer(inflight);
}
