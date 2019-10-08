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
		  int, unique_transaction transaction);
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
  bool destination_in_use = false;

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
				 int flags, unique_transaction transaction)
  : Inflight(req, true, std::move(transaction)),
    oldparent(oldparent), oldname(oldname),
    newparent(newparent), newname(newname), flags(flags)
{
}

Inflight_rename *Inflight_rename::reincarnate()
{
  Inflight_rename *x =
    new Inflight_rename(req, oldparent, oldname,
			newparent, newname, flags,
			std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_rename::commit_cb()
{
  return InflightAction::OK();
}

void erase_inode(FDBTransaction *transaction, fuse_ino_t ino)
{
  auto key_start = pack_inode_key(ino);
  auto key_stop = key_start;
  key_stop.push_back('\xff');

  fdb_transaction_clear_range(transaction,
			      key_start.data(), key_start.size(),
			      key_stop.data(),  key_stop.size());
}

InflightAction Inflight_rename::complicated()
{
  /**
   * If you couldn't tell from the method name, we're in the
   * complicated case for rename. We're in the case where we
   * have to unlink the destination, and then do our normal
   * work.
   * TODO should we do the rename work up in the main function
   * and then just somehow call unlink?
   */
  
  // remove the old dirent
  auto key = pack_dentry_key(oldparent, oldname);
  fdb_transaction_clear(transaction.get(),
			key.data(), key.size());

  FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_error_t err;

  err = fdb_future_get_keyvalue_array(inode_metadata_fetch.get(), (const FDBKeyValue **)&kvs, &kvcount, &more);
  if(err) return InflightAction::FDBError(err);
  if(kvcount < 1) {
    // referential integrity error; dirent points to missing inode
    return InflightAction::Abort(EIO);
  }
  // the first record had better be the inode
  FDBKeyValue inode_kv = kvs[0];

  INodeRecord inode;
  inode.ParseFromArray(inode_kv.value, inode_kv.value_length);
  if(!inode.IsInitialized()) {
    // well, bugger
    return InflightAction::Abort(EIO);
  }

  if(kvcount>1) {
    FDBKeyValue kv = kvs[1];
    auto key = reinterpret_cast<const uint8_t*>(kv.key);
    if((kv.key_length > (inode_key_length + 1)) &&
       key[inode_key_length] == 0x01) {
      // there's a use record present.
      destination_in_use = true;
    }
  }

  // TODO permissions checking on the whatever being removed
  
  if(directory_listing_fetch) {
    FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;
    fdb_error_t err;

    err = fdb_future_get_keyvalue_array(directory_listing_fetch.get(), (const FDBKeyValue **)&kvs, &kvcount, &more);
    if(err) return InflightAction::FDBError(err);
    if(kvcount>0) {
      // can't move over a directory with anything in it
      return InflightAction::Abort(ENOTEMPTY);
    }
  }

  // we always decrement. that'll take directories to
  // nlinks==1, which, if they linger around because
  // they were held open, is how other functions know
  // not to allow things to be created in the directory.
  inode.set_nlinks(inode.nlinks() - 1);
  // as such we always update the inode.
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
  
  if((directory_listing_fetch && (inode.nlinks()<=1)) ||
     (inode.nlinks() == 0)) {
    // if the nlinks has dropped low enough, we may be able
    // to erase the entire inode. even if we can't erase
    // the whole thing, we should mark it for garbage collection.
    
    // TODO locking?
    if(destination_in_use) {
      auto key = pack_garbage_key(inode.inode());
      uint8_t b = 0;
      fdb_transaction_set(transaction.get(),
			  key.data(), key.size(),
			  &b, 1);
    } else {
      erase_inode(transaction.get(), inode.inode());
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

  /****************************************************
   * Pull the futures over into DirectoryEntrys
   */
  {
    fdb_bool_t present;
    const uint8_t *val;
    int vallen;
    fdb_error_t err;

    err = fdb_future_get_value(origin_lookup.get(), &present, &val, &vallen);
    if(err) return InflightAction::FDBError(err);
    if(present)
      origin_dirent.ParseFromArray(val, vallen);

    err = fdb_future_get_value(destination_lookup.get(), &present, &val, &vallen);
    if(err) return InflightAction::FDBError(err);
    if(present)
      destination_dirent.ParseFromArray(val, vallen);
  }

  /****************************************************
   * Compare what the futures came back with, with the
   * stuff the flags say we need.
   * TODO probably also the place to check permissions.
   */
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
  }
#ifdef RENAME_NOREPLACE
  else if(flags == RENAME_NOREPLACE) {
    if(!origin_dirent.has_inode()) {
      return InflightAction::Abort(ENOENT);
    }
    if(destination_dirent.has_inode()) {
      return InflightAction::Abort(EEXIST);
    }
  }
#endif

  /****************************************************
   * We've established that we (so far) have all of the
   * information necessary to finish this request.
   */
  if(((flags == 0) && (!destination_dirent.has_inode()))
#ifdef RENAME_NOREPLACE
     || (flags == RENAME_NOREPLACE)
#endif
     ) {
    /**
     * This is the easy rename case. There's nothing at the
     * destination, so there's no risk of having to unlink
     * things.
     */

    // remove the old directory entry.
    auto key = pack_dentry_key(oldparent, oldname);
    fdb_transaction_clear(transaction.get(),
			  key.data(), key.size());

    // take the old directory entry contents, repack it.
    int olddirent_size = origin_dirent.ByteSize();
    uint8_t olddirent_buf[olddirent_size];
    origin_dirent.SerializeToArray(olddirent_buf, olddirent_size);

    // and save it into the new directory entry
    key = pack_dentry_key(newparent, newname);
    fdb_transaction_set(transaction.get(),
			key.data(), key.size(),
			olddirent_buf, olddirent_size);
  }
#ifdef RENAME_EXCHANGE
  else if(flags == RENAME_EXCHANGE) {
    /**
     * This case is only slightly more complicated than
     * the previous case. Here we swap the contents of the
     * two directory entries, but nothing is unlinked.
     */
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
    /**
     * This is the hard case. We're moving the origin
     * over top of an existing destination.
     * Since there's something at the destination, we'll
     * have to get rid of it.
     * TODO ugh can we share this code with unlink/rmdir?
     **/
    if(destination_dirent.type() == directory) {
      /**
       * The destination is a directory. We'll need to know
       * if it is empty before we can remove it.
       */
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

    /**
     * Regardless of what the destination is, we need to
     * fetch its inode and use records.
     */
    auto key_start = pack_inode_key(destination_dirent.inode());
    auto key_stop  = pack_inode_key(destination_dirent.inode());
    // this ensures we cover the use records, located at \x01
    key_stop.push_back('\x02');

    wait_on_future(fdb_transaction_get_range(transaction.get(),
					     key_start.data(),
					     key_start.size(), 0, 1,
					     key_stop.data(),
					     key_stop.size(),  0, 1,
					     // we don't care how many use
					     // records there are, we just
					     // need to know if there are
					     // 0, or >0. so, limit=2
					     2, 0,
					     FDB_STREAMING_MODE_WANT_ALL, 0,
					     0, 0),
		   &inode_metadata_fetch);
    return InflightAction::BeginWait(std::bind(&Inflight_rename::complicated, this));
  } else {
    return InflightAction::Abort(ENOSYS);
  }
  
  /**
   * If we've made it here, then we were in a simple case, and
   * we're all done except for the commit. So schedule that,
   * and head off to the commit callback when it finishes.
   */
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

  // TODO probably also need to fetch information about the parent inodes
  // for permissions checking.
  
  return std::bind(&Inflight_rename::check, this);
}

extern "C" void fdbfs_rename(fuse_req_t req,
			     fuse_ino_t parent, const char *name,
			     fuse_ino_t newparent, const char *newname)
{
  Inflight_rename *inflight =
    new Inflight_rename(req, parent, std::string(name),
			newparent, std::string(newname), 0,
			make_transaction());
  inflight->start();
}
