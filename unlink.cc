
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
#include "values.pb.h"

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
class Inflight_unlink_rmdir : Inflight {
public:
  Inflight_unlink_rmdir(fuse_req_t, fuse_ino_t, std::string, bool,
			FDBTransaction * = 0);
  Inflight_unlink_rmdir *reincarnate();
  void issue();
private:
  // for fetching the dirent given parent inode and path name
  unique_future dirent_lookup;
  // fetches inode metadata except xattrs
  unique_future inode_metadata_fetch;
  // fetches 0-1 of the directory entries in a directory
  unique_future directory_listing_fetch;
  unique_future commit;
  
  void postlookup();
  void inode_check();
  void rmdir_inode_dirlist_check();
  void commit_cb();
  
  // parent directory
  fuse_ino_t parent;
  // inode of the thing we're removing
  fuse_ino_t ino;
  // provided name and length
  std::string name;
  // true if we were called as rmdir
  bool actually_rmdir;
};

Inflight_unlink_rmdir::Inflight_unlink_rmdir(fuse_req_t req,
					     fuse_ino_t parent,
					     std::string name,
					     bool actually_rmdir,
					     FDBTransaction *transaction)
  : Inflight(req, true, transaction),
    parent(parent), name(name),
    actually_rmdir(actually_rmdir)
{
}

Inflight_unlink_rmdir *Inflight_unlink_rmdir::reincarnate()
{
  Inflight_unlink_rmdir *x =
    new Inflight_unlink_rmdir(req, parent, name, actually_rmdir,
			      transaction.release());
  delete this;
  return x;
}

void Inflight_unlink_rmdir::commit_cb()
{
  abort(0);
}

void Inflight_unlink_rmdir::rmdir_inode_dirlist_check()
{
  // got the directory listing future back, we can check to see if we're done.
  FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  if(fdb_future_get_keyvalue_array(directory_listing_fetch.get(),
				   (const FDBKeyValue **)&kvs,
				   &kvcount, &more)) {
    restart();
    return;
  }
  if(kvcount>0) {
    // can't rmdir a directory with any amount of stuff in it.
    abort(ENOTEMPTY);
    return;
  }
  
  // TODO check the metadata for permission to erase

  // we're a directory, so we can't have extra links, so this would
  // just be a user permissions test. we won't implement that yet.

  // dirent deletion (has to wait until we're sure we can remove the
  // entire thing)
  auto key = pack_dentry_key(parent, name);
  fdb_transaction_clear(transaction.get(), key.data(), key.size());

  auto key_start = pack_inode_key(ino);
  auto key_stop  = pack_inode_key(ino);
  // based on our KV layout, this will cover all inode records
  key_stop.push_back('\xff');
  
  fdb_transaction_clear_range(transaction.get(),
			      key_start.data(), key_start.size(),
			      key_stop.data(),  key_stop.size());

  // commit
  cb.emplace(std::bind(&Inflight_unlink_rmdir::commit_cb, this));
  wait_on_future(fdb_transaction_commit(transaction.get()),
		 &commit);
  begin_wait();
}

void Inflight_unlink_rmdir::inode_check()
{
  FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  if(fdb_future_get_keyvalue_array(inode_metadata_fetch.get(),
				   (const FDBKeyValue **)&kvs,
				   &kvcount, &more)) {
    restart();
    return;
  }
  // TODO check the metadata for permission to erase

  // find the inode record, should be the first kv pair
  if(kvcount<=0) {
    // uh. serious referential integrity error. some dirent pointed
    // at a non-existant inode.
    abort(EIO);
    return;
  }

  FDBKeyValue inode_kv = kvs[0];
  // TODO test the key to confirm this is actually the inode KV pair
  // we're just going to pretend for now that we found the right record
  INodeRecord inode;
  inode.ParseFromArray(inode_kv.value, inode_kv.value_length);
  if(!(inode.IsInitialized() && inode.has_nlinks())) {
    abort(EIO);
    return;
  }

  // check the stat structure
  // nlinks > 1? decrement and cleanup.
  if(inode.nlinks()>1) {
    inode.set_nlinks(inode.nlinks()-1);

    int inode_size = inode.ByteSize();
    uint8_t inode_buffer[inode_size];
    inode.SerializeToArray(inode_buffer, inode_size);
    
    fdb_transaction_set(transaction.get(),
		        static_cast<const uint8_t*>(inode_kv.key),
			inode_kv.key_length,
			inode_buffer, inode_size);
  } else {
    // nlinks == 1? we've removed the last dirent.

    // TODO actually perform these checks / do these things
    // zero locks? zero in-use records? clear the whole file.
    // otherwise, add an entry to the async garbage collection queue

    auto key_start = pack_inode_key(ino);
    auto key_stop  = pack_inode_key(ino);
    // based on our KV layout, this will cover all inode records
    key_stop.push_back('\xff');
  
    fdb_transaction_clear_range(transaction.get(),
				key_start.data(), key_start.size(),
				key_stop.data(),  key_stop.size());
  }

  // commit
  wait_on_future(fdb_transaction_commit(transaction.get()),
		 &commit);
  cb.emplace(std::bind(&Inflight_unlink_rmdir::commit_cb, this));
  begin_wait();
  return;
}

void Inflight_unlink_rmdir::postlookup()
{
  fdb_bool_t dirent_present;
  const uint8_t *value; int valuelen;
  if(fdb_future_get_value(dirent_lookup.get(), &dirent_present,
			  &value, &valuelen)) {
    restart();
    return;
  }

  if(!dirent_present) {
    abort(ENOENT);
    return;
  }

  filetype dirent_type;
  
  {
    DirectoryEntry dirent;
    dirent.ParseFromArray(value, valuelen);
    if(!dirent.IsInitialized()) {
      // bad record,
      abort(EIO);
      return;
    }

    ino = dirent.inode();
    dirent_type = dirent.type();
  }

  // check the values in the dirent to make sure
  // we're looking at the right kind of thing. bail
  // if it isn't the right thing.
  if(actually_rmdir) {
    // we want to find a directory
    if(dirent_type == directory) {
      // ok, we've successfully found something rmdir'able.

      // can't remove the dirent here, though, as there might be
      // dirents in the directory.

      auto start = pack_inode_key(ino);
      auto stop  = pack_inode_key(ino);
      // based on our KV layout, this will fetch all of the metadata
      // about the directory except the extended attributes.
      stop.push_back('\x01');

      wait_on_future(fdb_transaction_get_range(transaction.get(),
					       start.data(),
					       start.size(), 0, 1,
					       stop.data(),
					       stop.size(),  0, 1,
					       1000, 0,
					       FDB_STREAMING_MODE_WANT_ALL, 0,
					       0, 0),
		     &inode_metadata_fetch);

      // we want to scan for any directory entries inside of this
      // directory. so we'll produce a key from before the first
      // possible directory entry, and one for after the last
      // possible, and then get the range, limit 1.
      start = pack_inode_key(ino);
      start.push_back('d');
      start = pack_inode_key(ino);
      start.push_back('e');

      wait_on_future(fdb_transaction_get_range(transaction.get(),
					       start.data(),
					       start.size(), 0, 1,
					       stop.data(),
					       stop.size(),  0, 1,
					       1, 0,
					       FDB_STREAMING_MODE_WANT_ALL, 0,
					       0, 0),
		     &directory_listing_fetch);

      cb.emplace(std::bind(&Inflight_unlink_rmdir::rmdir_inode_dirlist_check, this));
      begin_wait();
      return;
    } else {
      // mismatch. bail.
      abort(ENOTDIR);
      return;
    }
  } else {
    // we want anything except a directory
    if(dirent_type != S_IFDIR) {
      // successfully found something unlinkable.
      auto key = pack_dentry_key(parent, name);
      fdb_transaction_clear(transaction.get(), key.data(), key.size());

      auto start = pack_inode_key(ino);
      auto stop  = pack_inode_key(ino);
      // based on our KV layout, this will fetch all of the metadata
      // about the file except the extended attributes.
      stop.push_back('\x01');

      // we'll use this to decrement st_nlink, check if it has reached zero
      // and if it has, and proceed with the plan.
      wait_on_future(fdb_transaction_get_range(transaction.get(),
					       start.data(),
					       start.size(), 0, 1,
					       stop.data(),
					       stop.size(),  0, 1,
					       1000, 0,
					       FDB_STREAMING_MODE_WANT_ALL, 0,
					       0, 0),
		     &inode_metadata_fetch);
      cb.emplace(std::bind(&Inflight_unlink_rmdir::inode_check, this));
      begin_wait();
      return;
    } else {
      // mismatch. bail.
      abort(EISDIR);
      return;
    }
  }
}

void Inflight_unlink_rmdir::issue()
{
  // TODO for correct permissions checking i think we need to also fetch
  // the inode of the containing directory so that we can see if we'll have
  // permission to remove the dirent.
  // that can run in parallel to this fetch, following the normal pattern.
  auto key = pack_dentry_key(parent, name);
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &dirent_lookup);
  cb.emplace(std::bind(&Inflight_unlink_rmdir::postlookup, this));
  begin_wait();
}

extern "C" void fdbfs_unlink(fuse_req_t req, fuse_ino_t ino, const char *name)
{
  Inflight_unlink_rmdir *inflight =
    new Inflight_unlink_rmdir(req, ino, name, false);
  inflight->issue();
}

extern "C" void fdbfs_rmdir(fuse_req_t req, fuse_ino_t ino, const char *name)
{
  Inflight_unlink_rmdir *inflight =
    new Inflight_unlink_rmdir(req, ino, name, true);
  inflight->issue();
}
