#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>

#include <linux/fs.h>

#include "util.h"
#include "inflight.h"
#include "values.pb.h"

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
class Inflight_rename : public Inflight {
public:
  Inflight_rename(fuse_req_t,
		  fuse_ino_t, std::string,
		  fuse_ino_t, std::string,
		  int, FDBTransaction * = 0);
  Inflight_rename *reincarnate();
  InflightCallback issue();
private:
  fuse_ino_t oldparent;
  std::string oldname;

  fuse_ino_t newparent;
  std::string newname;
  
  unsigned int flags;

  unique_future origin_lookup;
  DirectoryEntry origin_dirent;
  unique_future destination_lookup;
  DirectoryEntry destination_dirent;

  unique_future directory_listing_fetch;
  unique_future inode_metadata_fetch;

  unique_future commit;

  InflightAction check();
  InflightAction complicated();
  InflightAction commit_cb();
};

Inflight_rename::Inflight_rename(fuse_req_t req,
				 fuse_ino_t oldparent, std::string oldname,
				 fuse_ino_t newparent, std::string newname,
				 int flags, FDBTransaction *transaction)
  : Inflight(req, true, transaction),
    oldparent(oldparent), oldname(oldname),
    newparent(newparent), newname(newname), flags(flags)
{
}

Inflight_rename *Inflight_rename::reincarnate()
{
  Inflight_rename *x =
    new Inflight_rename(req, oldparent, oldname,
			newparent, newname, flags,
			transaction.release());
  delete this;
  return x;
}

InflightAction Inflight_rename::commit_cb()
{
  return InflightAction::OK();
}

InflightAction Inflight_rename::complicated()
{
  // remove the old dirent
  auto key = pack_dentry_key(oldparent, oldname);
  fdb_transaction_clear(transaction.get(),
			key.data(), key.size());

  if(directory_listing_fetch) {
    FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;
    if(fdb_future_get_keyvalue_array(directory_listing_fetch.get(),
				     (const FDBKeyValue **)&kvs,
				     &kvcount, &more)) {
      return InflightAction::Restart();
    }
    if(kvcount>0) {
      // can't move over a directory with anything in it
      return InflightAction::Abort(ENOTEMPTY);
    }

    // TODO permissions checking on the directory being replaced

    // erase the now-unused inode
    auto key_start = pack_inode_key(destination_dirent.inode());
    auto key_stop = pack_inode_key(destination_dirent.inode());
    key_stop.push_back('\xff');

    fdb_transaction_clear_range(transaction.get(),
				key_start.data(), key_start.size(),
				key_stop.data(),  key_stop.size());
  } else {
    // TODO handling of replacing a file

    FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;
    if(fdb_future_get_keyvalue_array(inode_metadata_fetch.get(),
				     (const FDBKeyValue **)&kvs,
				     &kvcount, &more)) {
      return InflightAction::Restart();
    }
    // the first record had better be the inode
    INodeRecord inode;
    inode.ParseFromArray(kvs[0].value, kvs[0].value_length);
    if(!inode.IsInitialized()) {
      // well, bugger
      return InflightAction::Abort(EIO);
    }
    FDBKeyValue inode_kv = kvs[0];

    if(inode.nlinks()>1) {
      inode.set_nlinks(inode.nlinks() - 1);
      struct timespec tv;
      clock_gettime(CLOCK_REALTIME, &tv);
      update_ctime(&inode, &tv);
      int inode_size = inode.ByteSize();
      uint8_t inode_buffer[inode_size];
      inode.SerializeToArray(inode_buffer, inode_size);
      
      fdb_transaction_set(transaction.get(),
			  static_cast<const uint8_t*>(inode_kv.key),
			  inode_kv.key_length,
			  inode_buffer, inode_size);
    } else {
      // nlinks == 1, we can erase it
      // TODO actually perform these checks / do these things
      // zero locks? zero in-use records? clear the whole file.
      // otherwise, add an entry to the async garbage collection queue

      auto key_start = pack_inode_key(inode.inode());
      auto key_stop  = pack_inode_key(inode.inode());
      // based on our KV layout, this will cover all inode records
      key_stop.push_back('\xff');
  
      fdb_transaction_clear_range(transaction.get(),
				  key_start.data(), key_start.size(),
				  key_stop.data(),  key_stop.size());
    }
  }
  
  // set the new dirent to the correct value
  key = pack_dentry_key(newparent, newname);
  int dirent_size = origin_dirent.ByteSize();
  uint8_t dirent_buffer[dirent_size];
  origin_dirent.SerializeToArray(dirent_buffer, dirent_size);
  fdb_transaction_set(transaction.get(),
		      key.data(), key.size(),
		      dirent_buffer, dirent_size);

  // commit
  wait_on_future(fdb_transaction_commit(transaction.get()),
		 &commit);

  return InflightAction::BeginWait(std::bind(&Inflight_rename::commit_cb, this));
}

InflightAction Inflight_rename::check()
{
  {
    fdb_bool_t present;
    const uint8_t *val;
    int vallen;

    if(fdb_future_get_value(origin_lookup.get(), &present, &val, &vallen)) {
      return InflightAction::Restart();
    }
    if(present)
      origin_dirent.ParseFromArray(val, vallen);
    
    if(fdb_future_get_value(destination_lookup.get(), &present, &val, &vallen)) {
      return InflightAction::Restart();
    }
    if(present)
      destination_dirent.ParseFromArray(val, vallen);
  }

  if(flags == 0) {
    // default. we want an origin, and don't care about existance
    // of the destination, yet.
    if(!origin_dirent.has_inode()) {
      return InflightAction::Abort(ENOENT);
    }
    // turns out you can move a directory on top of another,
    // empty directory. look to see if we're moving a directory
    if(origin_dirent.has_type() &&
       (origin_dirent.type() == directory) &&
       destination_dirent.has_type() &&
       (destination_dirent.type() != directory)) {
      return InflightAction::Abort(EISDIR);
    }
  } else if(flags == RENAME_EXCHANGE) {
    // need to both exist
    if((!origin_dirent.has_inode()) || (!destination_dirent.has_inode())) {
      return InflightAction::Abort(ENOENT);
    }
  } else if(flags == RENAME_NOREPLACE) {
    if(!origin_dirent.has_inode()) {
      return InflightAction::Abort(ENOENT);
    }
    if(destination_dirent.has_inode()) {
      return InflightAction::Abort(EEXIST);
    }
  }

  // ok, this is tenatively doable!
  if(((flags == 0) && (!destination_dirent.has_inode()))
#ifdef RENAME_NOREPLACE
     || (flags == RENAME_NOREPLACE)
#endif
     ) {
    // easy case. there's no risk of having to unlink things.
    int olddirent_size = origin_dirent.ByteSize();
    uint8_t olddirent_buf[olddirent_size];
    origin_dirent.SerializeToArray(olddirent_buf, olddirent_size);

    auto key = pack_dentry_key(oldparent, oldname);
    fdb_transaction_clear(transaction.get(),
			  key.data(), key.size());

    key = pack_dentry_key(newparent, newname);
    fdb_transaction_set(transaction.get(),
			key.data(), key.size(),
			olddirent_buf, olddirent_size);
  }
#ifdef RENAME_EXCHANGE
  else if(flags == RENAME_EXCHANGE) {
    // no problem, we're just rearranging dirents
    int olddirent_size = origin_dirent.ByteSize();
    uint8_t olddirent_buf[olddirent_size];
    origin_dirent.SerializeToArray(olddirent_buf, olddirent_size);

    int newdirent_size = destination_dirent.ByteSize();
    uint8_t newdirent_buf[newdirent_size];
    destination_dirent.SerializeToArray(newdirent_buf, newdirent_size);

    auto key = pack_dentry_key(oldparent, oldname);
    fdb_transaction_set(transaction.get(),
			key.data(), key.size(),
			newdirent_buf, newdirent_size);

    key = pack_dentry_key(newparent, newname);
    fdb_transaction_set(transaction.get(),
			key.data(), key.size(),
			olddirent_buf, olddirent_size);
  }
#endif
  else if(flags == 0) {
    // TODO hard case.
    // there's something in the destination, and we've got to get
    // rid of it.
    // TODO ugh can we share this code with unlink/rmdir?
    if(destination_dirent.type() == directory) {
      // ok, it's a directory. we need to check and see if there's
      // anything in it.
      auto key_start = pack_inode_key(destination_dirent.inode());
      key_start.push_back('d');
      auto key_stop  = pack_inode_key(destination_dirent.inode());
      key_stop.push_back('d' + 1);
      
      wait_on_future(fdb_transaction_get_range(transaction.get(),
					       key_start.data(),
					       key_start.size(), 0, 1,
					       key_stop.data(),
					       key_stop.size(),  0, 1,
					       1, 0,
					       FDB_STREAMING_MODE_WANT_ALL, 0,
					       0, 0),
		     &directory_listing_fetch);
    }

    auto key_start = pack_inode_key(destination_dirent.inode());
    auto key_stop  = pack_inode_key(destination_dirent.inode());
    key_stop.push_back('\x01');

    wait_on_future(fdb_transaction_get_range(transaction.get(),
					     key_start.data(),
					     key_start.size(), 0, 1,
					     key_stop.data(),
					     key_stop.size(),  0, 1,
					     1000, 0,
					     FDB_STREAMING_MODE_WANT_ALL, 0,
					     0, 0),
		   &inode_metadata_fetch);
    return InflightAction::BeginWait(std::bind(&Inflight_rename::complicated, this));
  } else {
    return InflightAction::Abort(ENOSYS);
  }
  
  // commit
  wait_on_future(fdb_transaction_commit(transaction.get()),
                 &commit);
  return InflightAction::BeginWait(std::bind(&Inflight_rename::commit_cb, this));
}

InflightCallback Inflight_rename::issue()
{
  auto key = pack_dentry_key(oldparent, oldname);

  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &origin_lookup);

  key = pack_dentry_key(newparent, newname);

  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &destination_lookup);

  return std::bind(&Inflight_rename::check, this);
}

extern "C" void fdbfs_rename(fuse_req_t req,
			     fuse_ino_t parent, const char *name,
			     fuse_ino_t newparent, const char *newname)
{
  std::string sname(name), snewname(newname);
  Inflight_rename *inflight =
    new Inflight_rename(req, parent, sname, newparent, snewname, 0);
  inflight->start();
}
