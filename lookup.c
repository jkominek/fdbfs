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
#include "values.pb-c.h"

/*************************************************************
 * lookup
 *************************************************************
 * INITIAL PLAN
 * not too bad. given a directory inode and a name, we:
 *   1. fetch the directory entry, getting us an inode.
 *   2. getattr-equiv on that inode.
 *
 * REAL PLAN
 * might be a good spot for an optimization? we can finish off
 * the fuse request, but then maintain some inode cache with
 * the last unchanging attributes of an inode that we've seen.
 * we could reject invalid requests to inodes faster. (readdir
 * on a file, for instance?) is it worth it to make the error
 * case faster?
 *
 * TRANSACTIONAL BEHAVIOR
 * We're doing the two reads as snapshots. Since the filesystem
 * can change arbitrarily immediately after we're done, it doesn't
 * much matter if it changes by a little or a lot. Just want to
 * ensure that we show the user something that was true.
 */
struct fdbfs_inflight_lookup {
  struct fdbfs_inflight_base base;

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

void fdbfs_lookup_inode(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_lookup *inflight = p;

  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  fdb_future_get_value(f, &present, (const uint8_t **)&val, &vallen);

  // and second callback, to get the attributes
  if(present) {
    struct fuse_entry_param e;

    e.ino = inflight->target;
    // TODO technically we need to be smarter about generations
    e.generation = 1;
    unpack_stat_from_dbvalue(val, vallen, &(e.attr));
    e.attr_timeout = 0.01;
    e.entry_timeout = 0.01;

    debug_print("fdbfs_lookup_callback returning entry for req %p ino %li\n", inflight->base.req, inflight->ino);
      
    fuse_reply_entry(inflight->base.req, &e);
  } else {
    debug_print("fdbfs_lookup_callback for req %p didn't find attributes\n", inflight->base.req);
    fuse_reply_err(inflight->base.req, EIO);
  }
  fdbfs_inflight_cleanup(p);
  fdb_future_destroy(f);
}

void fdbfs_lookup_dirent(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_lookup *inflight = p;

  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  fdb_future_get_value(f, &present, (const uint8_t **)&val, &vallen);

  // we're on the first callback, to get the directory entry
  if(present) {
    {
      DirectoryEntry *dirent;
      dirent = directory_entry__unpack(NULL, vallen, val);
      if(dirent == NULL) {
	// terrible error
      }
      if(!dirent->has_inode) {
	// more error
      }

      inflight->target = dirent->inode;

      directory_entry__free_unpacked(dirent, NULL);
    }

    // TODO ensure this is sized right.
    uint8_t key[512];
    int keylen;

    pack_inode_key(inflight->target, key, &keylen);

    // and request just that inode
    FDBFuture *f = fdb_transaction_get(inflight->base.transaction, key, keylen, 1);

    inflight->base.cb = fdbfs_lookup_inode;
    fdb_future_set_callback(f, fdbfs_error_checker, p);
  } else {
    debug_print("fdbfs_lookup_callback for req %p didn't find entry\n", inflight->base.req);
    fuse_reply_err(inflight->base.req, ENOENT);
    fdbfs_inflight_cleanup(p);
  }

  fdb_future_destroy(f);
}

void fdbfs_lookup_issuer(void *p)
{
  // find 'name' in the directory pointed to by 'parent'
  struct fdbfs_inflight_lookup *inflight = p;

  // TODO ensure this is sized right.
  uint8_t key[1024];
  int keylen;

  pack_dentry_key(inflight->ino, inflight->name, inflight->namelen,
		  key, &keylen);

#ifdef DEBUG
  char buffer[2048];
  char buffer2[2048];
  for(int i=0; i<keylen; i++)
    sprintf(buffer+(i<<1), "%02x", key[i]);
  bcopy(inflight->name, buffer2, inflight->namelen);
  buffer2[inflight->namelen] = '\0';
  
  debug_print("fdbfs_lookup_issuer req %p launching get for key %s ino %li name '%s'\n", inflight->base.req, buffer, inflight->ino, buffer2);
#endif
  
  FDBFuture *f = fdb_transaction_get(inflight->base.transaction, key, keylen, 1);

  inflight->base.cb = fdbfs_lookup_dirent;
  fdb_future_set_callback(f, fdbfs_error_checker, p);
}

void fdbfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  int namelen = strlen(name);

  // get the file attributes of an inode
  struct fdbfs_inflight_lookup *inflight;
  // to just make one allocation, we'll stuff our copy of the name
  // right after the struct.
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_lookup) + namelen + 1,
				   req,
				   fdbfs_lookup_dirent,
				   fdbfs_lookup_issuer,
				   T_READONLY);

  inflight->ino = parent;
  inflight->namelen = namelen;
  inflight->name = ((char*)inflight) + sizeof(struct fdbfs_inflight_lookup);

  strncpy(inflight->name, name, namelen+1);
  
  fdbfs_lookup_issuer(inflight);
}
