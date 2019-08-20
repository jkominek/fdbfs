
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
#include "values.pb-c.h"

/*************************************************************
 * unlink
 *************************************************************
 * INITIAL PLAN
 * got an inode for the directory, and the name. need to look
 * up probably a bunch of things, confirm there's permission
 * for the unlink(???) and that the thing in question can be
 * unlinked. if so, remove the dirent, reduce link count.
 *
 * link count at zero:
 * if use count is zero, clearrange the whole thing.
 * otherwise, put on the GC list to be checked asynchronously.
 *
 * REAL PLAN
 * ???
 */
struct fdbfs_inflight_unlink {
  struct fdbfs_inflight_base base;
  // for fetching the dirent given parent inode and path name
  FDBFuture *dirent_lookup;
  // fetches inode metadata except xattrs
  FDBFuture *inode_metadata_fetch;
  // fetches 0-1 of the directory entries in a directory
  FDBFuture *directory_listing_fetch;

  // parent directory
  fuse_ino_t parent;
  // inode of the thing we're removing
  fuse_ino_t ino;
  // provided name and length
  char *name;
  int namelen;
  // true if we were called as rmdir
  _Bool actually_rmdir;
};

void fdbfs_unlink_commit_cb(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_unlink *inflight = p;
  
  fuse_reply_err(inflight->base.req, 0);
  fdb_future_destroy(f);
  fdbfs_inflight_cleanup(p);
}

void fdbfs_rmdir_inode_dirlist_check(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_unlink *inflight = p;
  // want:
  if(!fdb_future_is_ready(inflight->directory_listing_fetch)) {
    fdb_future_set_callback(inflight->directory_listing_fetch, fdbfs_error_checker, p);
    return;
  }

  // got the directory listing future back, we can check to see if we're done.
  FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_future_get_keyvalue_array(inflight->directory_listing_fetch,
				(const FDBKeyValue **)&kvs, &kvcount, &more);
  if(kvcount>0) {
    // can't rmdir a directory with any amount of stuff in it.
    fuse_reply_err(inflight->base.req, ENOTEMPTY);
    fdb_future_destroy(inflight->directory_listing_fetch);
    fdb_future_destroy(inflight->inode_metadata_fetch);
    fdbfs_inflight_cleanup(p);
    return;
  }
  
  if(!fdb_future_is_ready(inflight->inode_metadata_fetch)) {
    fdb_future_set_callback(inflight->inode_metadata_fetch, fdbfs_error_checker, p);
    return;
  }

  fdb_future_get_keyvalue_array(inflight->directory_listing_fetch,
				(const FDBKeyValue **)&kvs, &kvcount, &more);
  // TODO check the metadata for permission to erase

  // we're a directory, so we can't have extra links, so this would
  // just be a user permissions test. we won't implement that yet.

  // dirent deletion (has to wait until we're sure we can remove the
  // entire thing)
  uint8_t key[512]; // TODO correct size
  int keylen;
  pack_dentry_key(inflight->parent, inflight->name, inflight->namelen, key, &keylen);
  printf("dentry we're going to clear: ");
  print_bytes(key, keylen);
  printf("\n");
  fdb_transaction_clear(inflight->base.transaction, key, keylen);

  uint8_t key_start[512], key_stop[512]; // TODO more correct sizes
  int key_startlen, key_stoplen;
  pack_inode_key(inflight->ino, key_start, &key_startlen);
  pack_inode_key(inflight->ino, key_stop,  &key_stoplen);
  // based on our KV layout, this will cover all inode records
  key_stop[key_stoplen] = '\xff';
  key_stoplen++;
  
  fdb_transaction_clear_range(inflight->base.transaction,
			      key_start, key_startlen,
			      key_stop, key_stoplen);

  // commit
  inflight->base.cb = fdbfs_unlink_commit_cb;
  fdb_future_set_callback(fdb_transaction_commit(inflight->base.transaction),
			  fdbfs_error_checker, p);
}

void fdbfs_unlink_inodecheck(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_unlink *inflight = p;

  FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_future_get_keyvalue_array(inflight->inode_metadata_fetch,
				(const FDBKeyValue **)&kvs, &kvcount, &more);
  // TODO check the metadata for permission to erase

  // find the inode record, should be the first kv pair
  if(kvcount<=0) {
    // uh. serious referential integrity error. some dirent pointed
    // at a non-existant inode.
    fuse_reply_err(inflight->base.req, EIO);
    fdb_future_destroy(f);
    fdbfs_inflight_cleanup(p);
  }

  FDBKeyValue inode_kv = kvs[0];
  // TODO test the key to confirm this is actually the inode KV pair
  // we're just going to pretend for now that we found the right record
  INodeRecord *inode = inode_record__unpack(NULL, inode_kv.value_length, inode_kv.value);
  if((inode==NULL) || (!inode->has_nlinks)) {
    // serious error
  }

  // check the stat structure
  // nlinks > 1? decrement and cleanup.
  if(inode->nlinks>1) {
    inode->nlinks -= 1;

    int inode_size = inode_record__get_packed_size(inode);
    uint8_t inode_buffer[inode_size];
    inode_record__pack(inode, inode_buffer);
    
    fdb_transaction_set(inflight->base.transaction,
		        inode_kv.key, inode_kv.key_length,
			inode_buffer, inode_size);
  } else {
    // nlinks == 1? we've removed the last dirent.

    // TODO actually perform these checks / do these things
    // zero locks? zero in-use records? clear the whole file.
    // otherwise, add an entry to the async garbage collection queue

    uint8_t key_start[512], key_stop[512]; // TODO more correct sizes
    int key_startlen, key_stoplen;
    pack_inode_key(inflight->ino, key_start, &key_startlen);
    pack_inode_key(inflight->ino, key_stop,  &key_stoplen);
    // based on our KV layout, this will cover all inode records
    key_stop[key_stoplen] = '\xff';
    key_stoplen++;
  
    fdb_transaction_clear_range(inflight->base.transaction,
				key_start, key_startlen,
				key_stop, key_stoplen);
  }

  // commit
  inflight->base.cb = fdbfs_unlink_commit_cb;
  fdb_future_set_callback(fdb_transaction_commit(inflight->base.transaction),
			  fdbfs_error_checker, p);

  inode_record__free_unpacked(inode, NULL);
  
  return;
}

void fdbfs_unlink_postlookup(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_unlink *inflight = p;

  fdb_bool_t dirent_present;
  const uint8_t *value; int valuelen;
  fdb_future_get_value(inflight->dirent_lookup, &dirent_present, &value, &valuelen);

  if(!dirent_present) {
    // can't unlink what isn't there!
    printf("failed to find dirent for ino %lx and in parent %lx (name '%s')\n", inflight->ino, inflight->parent, inflight->name);
    fuse_reply_err(inflight->base.req, ENOENT);
    fdb_future_destroy(inflight->dirent_lookup);
    fdbfs_inflight_cleanup(p);
    return;
  }

  Filetype dirent_type;
  
  {
    DirectoryEntry *dirent;
    dirent = directory_entry__unpack(NULL, valuelen, value);
    if(dirent == NULL) {
      // terrible error
    }
    if((!dirent->has_inode) || (!dirent->has_type)) {
        // more terrible errors
    }

    inflight->ino = dirent->inode;
    dirent_type = dirent->type;

    directory_entry__free_unpacked(dirent, NULL);
  }

  fdb_future_destroy(inflight->dirent_lookup);

  // check the values in the dirent to make sure
  // we're looking at the right kind of thing. bail
  // if it isn't the right thing.
  if(inflight->actually_rmdir) {
    // we want to find a directory
    if(dirent_type == S_IFDIR) {
      // ok, we've successfully found something rmdir'able.

      // can't remove the dirent here, though, as there might be
      // dirents in the directory.

      uint8_t key_start[512], key_stop[512]; // TODO more correct sizes
      int key_startlen, key_stoplen;
      pack_inode_key(inflight->ino, key_start, &key_startlen);
      pack_inode_key(inflight->ino, key_stop,  &key_stoplen);
      // based on our KV layout, this will fetch all of the metadata
      // about the directory except the extended attributes.
      key_stop[key_stoplen] = '\x01';
      key_stoplen++;
      inflight->inode_metadata_fetch =
	fdb_transaction_get_range(inflight->base.transaction,
				  key_start, key_startlen, 0, 1,
				  key_stop,  key_stoplen,  0, 1,
				  1000, 0,
				  FDB_STREAMING_MODE_WANT_ALL, 0,
				  0, 0);

      // we want to scan for any directory entries inside of this
      // directory. so we'll produce a key from before the first
      // possible directory entry, and one for after the last
      // possible, and then get the range, limit 1.
      pack_inode_key(inflight->ino, key_start, &key_startlen);
      key_start[key_startlen++] = 'd';
      pack_inode_key(inflight->ino, key_stop,  &key_stoplen);
      key_stop[key_stoplen++]   = 'e';

      inflight->directory_listing_fetch =
	fdb_transaction_get_range(inflight->base.transaction,
				  key_start, key_startlen, 0, 1,
				  key_stop,  key_stoplen,  0, 1,
				  1, 0,
				  FDB_STREAMING_MODE_WANT_ALL, 0,
				  0, 0);

      inflight->base.cb = fdbfs_rmdir_inode_dirlist_check;
      // only set callback on one of the futures; it'll chain to the other.
      fdb_future_set_callback(inflight->inode_metadata_fetch, fdbfs_error_checker, p);
      return;
    } else {
      // mismatch. bail.
      fuse_reply_err(inflight->base.req,  ENOTDIR);
      fdbfs_inflight_cleanup(p);
      return;
    }
  } else {
    // we want anything except a directory
    if(dirent_type != S_IFDIR) {
      // successfully found something unlinkable.
      uint8_t key[512]; // TODO correct size
      int keylen;
      pack_dentry_key(inflight->parent, inflight->name, inflight->namelen, key, &keylen);
      printf("dentry we're going to clear: ");
      print_bytes(key, keylen);
      printf("\n");
      fdb_transaction_clear(inflight->base.transaction, key, keylen);

      uint8_t key_start[512], key_stop[512]; // TODO more correct sizes
      int key_startlen, key_stoplen;
      pack_inode_key(inflight->ino, key_start, &key_startlen);
      pack_inode_key(inflight->ino, key_stop,  &key_stoplen);
      // based on our KV layout, this will fetch all of the metadata
      // about the file except the extended attributes.
      key_stop[key_stoplen] = '\x01';
      key_stoplen++;
      inflight->inode_metadata_fetch =
	fdb_transaction_get_range(inflight->base.transaction,
				  key_start, key_startlen, 0, 1,
				  key_stop,  key_stoplen,  0, 1,
				  1000, 0,
				  FDB_STREAMING_MODE_WANT_ALL, 0,
				  0, 0);
      // we'll use this to decrement st_nlink, check if it has reached zero
      // and if it has, and proceed with the plan.
      inflight->base.cb = fdbfs_unlink_inodecheck;
      fdb_future_set_callback(inflight->inode_metadata_fetch, fdbfs_error_checker, p);
      return;
    } else {
      // mismatch. bail.
      fuse_reply_err(inflight->base.req,  EISDIR);
      fdbfs_inflight_cleanup(p);
      return;
    }
  }
}

void fdbfs_unlink_issuelookup(void *p)
{
  struct fdbfs_inflight_unlink *inflight = p;

  // reset this in case commit failed
  inflight->base.cb = fdbfs_unlink_postlookup;

  // pack the dentry key
  uint8_t key[512]; // TODO correct size; should be function of name length
  int keylen;

  // TODO for correct permissions checking i think we need to also fetch
  // the inode of the containing directory so that we can see if we'll have
  // permission to remove the dirent.
  // that can run in parallel to this fetch, following the normal pattern.
  pack_dentry_key(inflight->parent, inflight->name, strlen(inflight->name), key, &keylen);
  inflight->dirent_lookup = fdb_transaction_get(inflight->base.transaction, key, keylen, 0);

  fdb_future_set_callback(inflight->dirent_lookup, fdbfs_error_checker, p);
}

void _fdbfs_unlink_rmdir(fuse_req_t req, fuse_ino_t ino, const char *name,
			 _Bool actually_rmdir)
{
  // get the file attributes of an inode
  struct fdbfs_inflight_unlink *inflight;
  int namelen = strlen(name);
  inflight = fdbfs_inflight_create(sizeof(struct fdbfs_inflight_unlink) +
				   namelen + 1,
				   req,
				   fdbfs_unlink_postlookup,
				   fdbfs_unlink_issuelookup,
				   T_READWRITE);
  inflight->parent = ino;
  inflight->name = ((char *)inflight) + sizeof(struct fdbfs_inflight_unlink);
  inflight->namelen = namelen;
  inflight->actually_rmdir = actually_rmdir;
  bcopy(name, inflight->name, namelen); // TODO smarter?
  inflight->name[namelen] = 0;

  debug_print("fdbfs_unlink taking off for req %p\n", inflight->base.req);
  
  fdbfs_unlink_issuelookup(inflight);
}

void fdbfs_unlink(fuse_req_t req, fuse_ino_t ino, const char *name)
{
  _fdbfs_unlink_rmdir(req, ino, name, 0);
}

void fdbfs_rmdir(fuse_req_t req, fuse_ino_t ino, const char *name)
{
  _fdbfs_unlink_rmdir(req, ino, name, 1);
}
