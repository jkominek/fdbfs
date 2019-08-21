#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>

#include <linux/fs.h>

#include "util.h"
#include "inflight.h"
#include "values.pb-c.h"

/*************************************************************
 * rename
 *************************************************************
 * INITIAL PLAN
 * check that the provided inode isn't a directory.
 * check that the provided destination is a directory.
 * construct a dirent and write it into the correct spot.
 *
 * REAL PLAN
 * ??
 *
 * TRANSACTIONAL BEHAVIOR
 * nothing special
 */
struct fdbfs_inflight_rename {
  struct fdbfs_inflight_base base;

  fuse_ino_t oldparent;
  char *oldname;
  int oldnamelen;

  fuse_ino_t newparent;
  char *newname;
  int newnamelen;
  
  unsigned int flags;

  FDBFuture *origin_lookup;
  DirectoryEntry *origin_dirent;
  FDBFuture *destination_lookup;
  DirectoryEntry *destination_dirent;

  FDBFuture *directory_listing_fetch;
  FDBFuture *inode_metadata_fetch;
};

void fdbfs_inflight_rename_cleanup(struct fdbfs_inflight_rename *inflight)
{
  if(inflight->origin_lookup)
    fdb_future_destroy(inflight->origin_lookup);
  if(inflight->destination_lookup)
    fdb_future_destroy(inflight->destination_lookup);
  if(inflight->directory_listing_fetch)
    fdb_future_destroy(inflight->directory_listing_fetch);
  if(inflight->inode_metadata_fetch)
    fdb_future_destroy(inflight->inode_metadata_fetch);
  if(inflight->origin_dirent)
    directory_entry__free_unpacked(inflight->origin_dirent, NULL);
  if(inflight->destination_dirent)
    directory_entry__free_unpacked(inflight->destination_dirent, NULL);
  fdbfs_inflight_cleanup((void*)inflight);
}

void fdbfs_rename_commit_cb(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_rename *inflight = p;

  fuse_reply_err(inflight->base.req, 0);

  fdb_future_destroy(f);
  fdbfs_inflight_rename_cleanup(inflight);
}

void fdbfs_rename_complicated(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_rename *inflight = p;

  if(!fdb_future_is_ready(inflight->inode_metadata_fetch)) {
    fdb_future_set_callback(inflight->inode_metadata_fetch,
			    fdbfs_error_checker, p);
    return;
  }

  if(inflight->directory_listing_fetch &&
     (!fdb_future_is_ready(inflight->directory_listing_fetch))) {
    fdb_future_set_callback(inflight->directory_listing_fetch,
			    fdbfs_error_checker, p);
    return;
  }

  uint8_t key[512]; // TODO correct size
  int keylen;

  // remove the old dirent
  pack_dentry_key(inflight->oldparent,
		  inflight->oldname, inflight->oldnamelen,
		  key, &keylen);
  fdb_transaction_clear(inflight->base.transaction,
			key, keylen);

  if(inflight->directory_listing_fetch) {
    FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;
    fdb_future_get_keyvalue_array(inflight->directory_listing_fetch,
				  (const FDBKeyValue **)&kvs, &kvcount, &more);
    if(kvcount>0) {
      // can't move over a directory with anything in it
      fuse_reply_err(inflight->base.req, ENOTEMPTY);
      fdbfs_inflight_rename_cleanup(p);
      return;
    }

    // TODO permissions checking on the directory being replaced

    // erase the now-unused inode
    uint8_t key_stop[512];
    int key_stoplen;
    pack_inode_key(inflight->destination_dirent->inode,
		   key, &keylen);
    pack_inode_key(inflight->destination_dirent->inode,
		   key_stop, &key_stoplen);
    key_stop[key_stoplen] = '\xff';
    key_stoplen++;

    fdb_transaction_clear_range(inflight->base.transaction,
				key, keylen,
				key_stop, key_stoplen);
  } else {
    // TODO handling of replacing a file

    FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;
    fdb_future_get_keyvalue_array(inflight->inode_metadata_fetch,
				  (const FDBKeyValue **)&kvs, &kvcount, &more);
    // the first record had better be the inode
    INodeRecord *inode =
      inode_record__unpack(NULL, kvs[0].value_length, kvs[0].value);
    if((inode == NULL) || (!inode->has_nlinks)) {
      // well, bugger
      fuse_reply_err(inflight->base.req, EIO);
      inode_record__free_unpacked(inode, NULL);
      fdbfs_inflight_rename_cleanup(inflight);
      return;
    }
    FDBKeyValue inode_kv = kvs[0];

    if(inode->nlinks>1) {
      inode->nlinks -= 1;
      int inode_size = inode_record__get_packed_size(inode);
      uint8_t inode_buffer[inode_size];
      inode_record__pack(inode, inode_buffer);
      
      fdb_transaction_set(inflight->base.transaction,
			  inode_kv.key, inode_kv.key_length,
			  inode_buffer, inode_size);
    } else {
      // nlinks == 1, we can erase it
      // TODO actually perform these checks / do these things
      // zero locks? zero in-use records? clear the whole file.
      // otherwise, add an entry to the async garbage collection queue

      uint8_t key_start[512], key_stop[512]; // TODO more correct sizes
      int key_startlen, key_stoplen;
      pack_inode_key(inode->inode, key_start, &key_startlen);
      pack_inode_key(inode->inode, key_stop,  &key_stoplen);
      // based on our KV layout, this will cover all inode records
      key_stop[key_stoplen] = '\xff';
      key_stoplen++;
  
      fdb_transaction_clear_range(inflight->base.transaction,
				  key_start, key_startlen,
				  key_stop, key_stoplen);
    }
    inode_record__free_unpacked(inode, NULL);
  }
  
  // set the new dirent to the correct value
  pack_dentry_key(inflight->newparent,
		  inflight->newname, inflight->newnamelen,
		  key, &keylen);
  int dirent_size = directory_entry__get_packed_size(inflight->origin_dirent);
  uint8_t dirent_buffer[dirent_size];
  directory_entry__pack(inflight->origin_dirent, dirent_buffer);
  fdb_transaction_set(inflight->base.transaction,
		      key, keylen,
		      dirent_buffer, dirent_size);

  // commit
  inflight->base.cb = fdbfs_rename_commit_cb;
  fdb_future_set_callback(fdb_transaction_commit(inflight->base.transaction),
			  fdbfs_error_checker, p);
  return;
}

void fdbfs_rename_check(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_rename *inflight = p;

  if(!fdb_future_is_ready(inflight->origin_lookup)) {
    fdb_future_set_callback(inflight->origin_lookup,
			    fdbfs_error_checker, p);
    return;
  }

  if(!fdb_future_is_ready(inflight->destination_lookup)) {
    fdb_future_set_callback(inflight->destination_lookup,
			    fdbfs_error_checker, p);
    return;
  }

  {
    fdb_bool_t present;
    const uint8_t *val;
    int vallen;

    fdb_future_get_value(inflight->origin_lookup, &present, &val, &vallen);
    if(present)
      inflight->origin_dirent = directory_entry__unpack(NULL, vallen, val);
    
    fdb_future_get_value(inflight->destination_lookup, &present, &val, &vallen);
    if(present)
      inflight->destination_dirent = directory_entry__unpack(NULL, vallen, val);
  }

  int err = 0;
  if(inflight->flags == 0) {
    // default. we want an origin, and don't care about existance
    // of the destination, yet.
    if(!inflight->origin_dirent) {
      err = ENOENT;
    }
    // turns out you can move a directory on top of another,
    // empty directory. look to see if we're moving a directory
    if(inflight->origin_dirent &&
       (inflight->origin_dirent->type == FILETYPE__directory) &&
       inflight->destination_dirent &&
       (inflight->destination_dirent->type != FILETYPE__directory)) {
      err = EISDIR;
    }
  } else if(inflight->flags == RENAME_EXCHANGE) {
    // need to both exist
    if((!inflight->origin_dirent) || (!inflight->destination_dirent)) {
      err = ENOENT;
    }
  } else if(inflight->flags == RENAME_NOREPLACE) {
    if(!inflight->origin_dirent) {
      err = ENOENT;
    }
    if(inflight->destination_dirent) {
      err = EEXIST;
    }
  }

  if(err) {
    fuse_reply_err(inflight->base.req, err);
    fdbfs_inflight_rename_cleanup(p);
    return;
  }

  // ok, this is tenatively doable!
  if(((inflight->flags == 0) && (!inflight->destination_dirent))
#ifdef RENAME_NOREPLACE
     || (inflight->flags == RENAME_NOREPLACE)
#endif
     ) {
    // easy case. there's no risk of having to unlink things.
    uint8_t key[2048];
    int keylen;
    
    int olddirent_size = directory_entry__get_packed_size(inflight->origin_dirent);
    uint8_t olddirent_buf[olddirent_size];
    directory_entry__pack(inflight->origin_dirent, olddirent_buf);

    pack_dentry_key(inflight->oldparent,
		    inflight->oldname, inflight->oldnamelen,
		    key, &keylen);
    fdb_transaction_clear(inflight->base.transaction,
			  key, keylen);

    pack_dentry_key(inflight->newparent,
		    inflight->newname, inflight->newnamelen,
		    key, &keylen);
    fdb_transaction_set(inflight->base.transaction,
			  key, keylen,
			  olddirent_buf, olddirent_size);
  }
#ifdef RENAME_EXCHANGE
  else if(inflight->flags == RENAME_EXCHANGE) {
    // no problem, we're just rearranging dirents
    int olddirent_size = directory_entry__get_packed_size(inflight->origin_dirent);
    uint8_t olddirent_buf[olddirent_size];
    directory_entry__pack(inflight->origin_dirent, olddirent_buf);

    int newdirent_size = directory_entry__get_packed_size(inflight->destination_dirent);
    uint8_t newdirent_buf[newdirent_size];
    directory_entry__pack(inflight->destination_dirent, newdirent_buf);

    uint8_t key[2048];
    int keylen;
    
    pack_dentry_key(inflight->oldparent,
		    inflight->oldname, inflight->oldnamelen,
		    key, &keylen);
    fdb_transaction_set(inflight->base.transaction,
			  key, keylen,
			  newdirent_buf, newdirent_size);

    pack_dentry_key(inflight->newparent,
		    inflight->newname, inflight->newnamelen,
		    key, &keylen);
    fdb_transaction_set(inflight->base.transaction,
			  key, keylen,
			  olddirent_buf, olddirent_size);
  }
#endif
  else if(inflight->flags == 0) {
    uint8_t key_start[512], key_stop[512]; // TODO more correct sizes         
    int key_startlen, key_stoplen;
    // TODO hard case.
    // there's something in the destination, and we've got to get
    // rid of it.
    // TODO ugh can we share this code with unlink/rmdir?
    if(inflight->destination_dirent->type == FILETYPE__directory) {
      // ok, it's a directory. we need to check and see if there's
      // anything in it.
      pack_inode_key(inflight->destination_dirent->inode, key_start, &key_startlen);
      key_start[key_startlen++] = 'd';
      pack_inode_key(inflight->destination_dirent->inode, key_stop,  &key_stoplen);
      key_stop[key_stoplen++]   = 'e';
      
      inflight->directory_listing_fetch =
	fdb_transaction_get_range(inflight->base.transaction,
				  key_start, key_startlen, 0, 1,
				  key_stop,  key_stoplen,  0, 1,
				  1, 0,
				  FDB_STREAMING_MODE_WANT_ALL, 0,
				  0, 0);
    }

    pack_inode_key(inflight->destination_dirent->inode, key_start, &key_startlen);
    pack_inode_key(inflight->destination_dirent->inode, key_stop,  &key_stoplen);
    key_stop[key_stoplen] = '\x01';
    key_stoplen++;

    inflight->inode_metadata_fetch =
      fdb_transaction_get_range(inflight->base.transaction,
				key_start, key_startlen, 0, 1,
				key_stop,  key_stoplen,  0, 1,
				1000, 0,
				FDB_STREAMING_MODE_WANT_ALL, 0,
				0, 0);
    inflight->base.cb = fdbfs_rename_complicated;
    fdb_future_set_callback(inflight->inode_metadata_fetch,
			    fdbfs_error_checker, p);
    return;
  } else {
    fuse_reply_err(inflight->base.req, ENOSYS);
    fdbfs_inflight_cleanup(p);
    return;
  }
  
  // commit
  inflight->base.cb = fdbfs_rename_commit_cb;
  fdb_future_set_callback(fdb_transaction_commit(inflight->base.transaction),
			  fdbfs_error_checker, p);
  return;

}

void fdbfs_rename_issuer(void *p)
{
  struct fdbfs_inflight_rename *inflight = p;

  uint8_t key[2048]; // TODO size
  int keylen;
  
  pack_dentry_key(inflight->oldparent,
		  inflight->oldname, inflight->oldnamelen,
		  key, &keylen);

  inflight->origin_lookup =
    fdb_transaction_get(inflight->base.transaction,
			key, keylen, 0);

  pack_dentry_key(inflight->newparent,
		  inflight->newname, inflight->newnamelen,
		  key, &keylen);

  inflight->destination_lookup =
    fdb_transaction_get(inflight->base.transaction,
			key, keylen, 0);

  inflight->base.cb = fdbfs_rename_check;
  fdb_future_set_callback(inflight->origin_lookup,
			  fdbfs_error_checker, p);
}

void fdbfs_rename(fuse_req_t req,
		  fuse_ino_t parent, const char *name,
		  fuse_ino_t newparent, const char *newname)
{
  int oldnamelen = strlen(name);
  int newnamelen = strlen(newname);

  struct fdbfs_inflight_rename *inflight;
  // to just make one allocation, we'll stuff our copy of the name
  // right after the struct.
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_rename) +
				   oldnamelen + 1 +
				   newnamelen + 1,
				   req,
				   fdbfs_rename_check,
				   fdbfs_rename_issuer,
				   T_READWRITE);

  inflight->oldparent = parent;
  inflight->oldnamelen = oldnamelen;
  inflight->oldname = ((char*)inflight) + sizeof(struct fdbfs_inflight_rename);

  inflight->newparent = newparent;
  inflight->newnamelen = newnamelen;
  inflight->newname = inflight->oldname + oldnamelen + 1;

  bcopy(name,    inflight->oldname, inflight->oldnamelen+1);
  bcopy(newname, inflight->newname, inflight->newnamelen+1);

  inflight->flags = 0;
  
  fdbfs_rename_issuer(inflight);
}
