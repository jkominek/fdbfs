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
 * lookup
 *************************************************************
 * INITIAL PLAN
 * pretty easy? given an inode and a name, we just need to do
 * a single get to find the inode that points to.
 *
 * REAL PLAN
 * might be a good spot for an optimization? we can finish off
 * the fuse request, but then maintain some inode cache with
 * the last unchanging attributes of an inode that we've seen.
 * we could reject invalid requests to inodes faster. (readdir
 * on a file, for instance?) is that worth it?
 */
struct fdbfs_inflight_lookup {
  struct fdbfs_inflight_base base;
  bool have_target;

  // stage 1
  fuse_ino_t ino;
  int namelen;
  // i guess we could build up the key and store it in here,
  // but that probably won't fit the pattern. we'll see what
  // the pattern ends up being, though.
  char *name;
  // stage 2
  fuse_ino_t target;
  
};

void fdbfs_lookup_callback(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_lookup *inflight = p;

  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  fdb_future_get_value(f, &present, (const uint8_t **)&val, &vallen);

  if(!inflight->have_target) {
    // we're on the first callback, to get the directory entry
    if(present) {
      bcopy(&(inflight->target), val, sizeof(fuse_ino_t));
      inflight->have_target = 1;

      // ok, now look up the inode attributes
      uint8_t key[512];
      int keylen;

      pack_inode_key(inflight->ino, key, &keylen);

      // and request just that inode
      FDBFuture *f = fdb_transaction_get_key(inflight->base.transaction, key, keylen, 0, 0, 0);

      fdb_future_set_callback(f, fdbfs_error_checker, p);
    } else {
      fuse_reply_err(inflight->base.req, ENOENT);
      fdbfs_inflight_cleanup(p);
    }
  } else {
    // and second callback, to get the attributes
    struct fuse_entry_param e;

    e.ino = inflight->ino;
    // TODO technically we need to be smarter about generations
    e.generation = 0;
    unpack_stat_from_dbvalue(val, vallen, &(e.attr));
    e.attr_timeout = 0.0;
    e.entry_timeout = 0.0;

    fuse_reply_entry(inflight->base.req, &e);
    fdbfs_inflight_cleanup(p);
  }  
  fdb_future_destroy(f);
}

void fdbfs_lookup_issuer(void *p)
{
  // find 'name' in the directory pointed to by 'parent'
  struct fdbfs_inflight_lookup *inflight = p;

  // pack the inode key
  uint8_t key[1024];
  int keylen;

  // who knows how many times we have to try this?
  // clear out any state
  inflight->have_target = 0;

  pack_dentry_key(inflight->ino, inflight->name, inflight->namelen,
		  key, &keylen);

  FDBFuture *f = fdb_transaction_get_key(inflight->base.transaction, key, keylen, 0, 0, 0);

  fdb_future_set_callback(f, fdbfs_error_checker, p);
}

void fdbfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  int namelen = strlen(name);

  // get the file attributes of an inode
  struct fdbfs_inflight_lookup *inflight;
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_lookup) + namelen + 1,
				   req,
				   fdbfs_lookup_callback,
				   fdbfs_lookup_issuer);
  inflight->have_target = 0;
  inflight->ino = parent;
  inflight->namelen = namelen;
  inflight->name = ((char*)inflight) + sizeof(struct fdbfs_inflight_lookup);

  strncpy(inflight->name, name, namelen+1);
  
  fdbfs_lookup_issuer(inflight);
}
