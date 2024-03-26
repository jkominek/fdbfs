#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <linux/fs.h>

#include "fdbfs_ops.h"
#include "inflight.h"
#include "util.h"
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
  Inflight_rename(fuse_req_t, fuse_ino_t, std::string, fuse_ino_t, std::string,
                  int, unique_transaction transaction);
  Inflight_rename *reincarnate();
  InflightCallback issue();

private:
  fuse_ino_t oldparent;
  std::string oldname;

  fuse_ino_t newparent;
  std::string newname;

  unsigned int flags;

  unique_future oldparent_inode_lookup;
  unique_future newparent_inode_lookup;

  unique_future origin_lookup;
  DirectoryEntry origin_dirent;
  unique_future destination_lookup;
  DirectoryEntry destination_dirent;
  bool destination_in_use = false;

  unique_future directory_listing_fetch;
  unique_future inode_metadata_fetch;

  InflightAction check();
  InflightAction complicated();
};

Inflight_rename::Inflight_rename(fuse_req_t req, fuse_ino_t oldparent,
                                 std::string oldname, fuse_ino_t newparent,
                                 std::string newname, int flags,
                                 unique_transaction transaction)
    : Inflight(req, ReadWrite::Yes, std::move(transaction)),
      oldparent(oldparent), oldname(oldname), newparent(newparent),
      newname(newname), flags(flags) {}

Inflight_rename *Inflight_rename::reincarnate() {
  Inflight_rename *x =
      new Inflight_rename(req, oldparent, oldname, newparent, newname, flags,
                          std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_rename::complicated() {
  /**
   * If you couldn't tell from the method name, we're in the
   * complicated case for rename. We're in the case where we
   * have to unlink the destination, and then do our normal
   * work.
   * TODO should we do the rename work up in the main function
   * and then just somehow call unlink?
   */

  // remove the old dirent
  {
    const auto key = pack_dentry_key(oldparent, oldname);
    fdb_transaction_clear(transaction.get(), key.data(), key.size());
  }

  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_error_t err;

  err = fdb_future_get_keyvalue_array(inode_metadata_fetch.get(), &kvs,
                                      &kvcount, &more);
  if (err)
    return InflightAction::FDBError(err);
  if (kvcount < 1) {
    // referential integrity error; dirent points to missing inode
    return InflightAction::Abort(EIO);
  }
  // the first record had better be the inode
  FDBKeyValue inode_kv = kvs[0];

  INodeRecord inode;
  inode.ParseFromArray(inode_kv.value, inode_kv.value_length);
  if (!inode.IsInitialized()) {
    // well, bugger
    return InflightAction::Abort(EIO);
  }

  if (kvcount > 1) {
    FDBKeyValue kv = kvs[1];
    if ((kv.key_length > (inode_key_length + 1)) &&
        kv.key[inode_key_length] == 0x01) {
      // there's a use record present.
      destination_in_use = true;
    }
  }

  // TODO permissions checking on the whatever being removed

  if (directory_listing_fetch) {
    const FDBKeyValue *kvs;
    int kvcount;
    fdb_bool_t more;
    fdb_error_t err;

    err = fdb_future_get_keyvalue_array(directory_listing_fetch.get(), &kvs,
                                        &kvcount, &more);
    if (err)
      return InflightAction::FDBError(err);
    if (kvcount > 0) {
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
  const int inode_size = inode.ByteSizeLong();
  uint8_t inode_buffer[inode_size];
  inode.SerializeToArray(inode_buffer, inode_size);

  fdb_transaction_set(transaction.get(), inode_kv.key, inode_kv.key_length,
                      inode_buffer, inode_size);

  if ((directory_listing_fetch && (inode.nlinks() <= 1)) ||
      (inode.nlinks() == 0)) {
    // if the nlinks has dropped low enough, we may be able
    // to erase the entire inode. even if we can't erase
    // the whole thing, we should mark it for garbage collection.

    // TODO locking?
    if (destination_in_use) {
      const auto key = pack_garbage_key(inode.inode());
      const uint8_t b = 0;
      fdb_transaction_set(transaction.get(), key.data(), key.size(), &b, 1);
    } else {
      erase_inode(transaction.get(), inode.inode());
    }
  }

  // set the new dirent to the correct value
  {
    const auto key = pack_dentry_key(newparent, newname);
    const int dirent_size = origin_dirent.ByteSizeLong();
    uint8_t dirent_buffer[dirent_size];
    origin_dirent.SerializeToArray(dirent_buffer, dirent_size);
    fdb_transaction_set(transaction.get(), key.data(), key.size(),
                        dirent_buffer, dirent_size);
  }

  return commit(InflightAction::OK);
}

InflightAction Inflight_rename::check() {

  /****************************************************
   * Pull the futures over into DirectoryEntrys
   */
  {
    fdb_bool_t present;
    const uint8_t *val;
    int vallen;
    fdb_error_t err;

    err = fdb_future_get_value(origin_lookup.get(), &present, &val, &vallen);
    if (err)
      return InflightAction::FDBError(err);
    if (present)
      origin_dirent.ParseFromArray(val, vallen);

    err =
        fdb_future_get_value(destination_lookup.get(), &present, &val, &vallen);
    if (err)
      return InflightAction::FDBError(err);
    if (present)
      destination_dirent.ParseFromArray(val, vallen);
  }

  {
    fdb_bool_t present;
    const uint8_t *val;
    int vallen;
    fdb_error_t err;

    err = fdb_future_get_value(oldparent_inode_lookup.get(), &present, &val,
                               &vallen);
    if (err)
      return InflightAction::FDBError(err);
    if (!present)
      return InflightAction::Abort(ENOENT);

    INodeRecord oldparent;
    oldparent.ParseFromArray(val, vallen);
    update_directory_times(transaction.get(), oldparent);

    err = fdb_future_get_value(newparent_inode_lookup.get(), &present, &val,
                               &vallen);
    if (err)
      return InflightAction::FDBError(err);
    if (!present)
      return InflightAction::Abort(ENOENT);

    INodeRecord newparent;
    newparent.ParseFromArray(val, vallen);
    if (oldparent.inode() != newparent.inode())
      update_directory_times(transaction.get(), newparent);
  }

  /****************************************************
   * Compare what the futures came back with, with the
   * stuff the flags say we need.
   * TODO probably also the place to check permissions.
   */
  if (flags == 0) {
    // default. we want an origin, and don't care about existance
    // of the destination, yet.
    if (!origin_dirent.has_inode()) {
      return InflightAction::Abort(ENOENT);
    }
    // turns out you can move a directory on top of another,
    // empty directory. look to see if we're moving a directory
    if (origin_dirent.has_type() && (origin_dirent.type() == ft_directory) &&
        destination_dirent.has_type() &&
        (destination_dirent.type() != ft_directory)) {
      return InflightAction::Abort(EISDIR);
    }
  } else if (flags == RENAME_EXCHANGE) {
    // need to both exist
    if ((!origin_dirent.has_inode()) || (!destination_dirent.has_inode())) {
      return InflightAction::Abort(ENOENT);
    }
  }
#ifdef RENAME_NOREPLACE
  else if (flags == RENAME_NOREPLACE) {
    if (!origin_dirent.has_inode()) {
      return InflightAction::Abort(ENOENT);
    }
    if (destination_dirent.has_inode()) {
      return InflightAction::Abort(EEXIST);
    }
  }
#endif

  /****************************************************
   * We've established that we (so far) have all of the
   * information necessary to finish this request.
   */
  if (((flags == 0) && (!destination_dirent.has_inode()))
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
    {
      const auto key = pack_dentry_key(oldparent, oldname);
      fdb_transaction_clear(transaction.get(), key.data(), key.size());
    }

    // take the old directory entry contents, repack it.
    const int olddirent_size = origin_dirent.ByteSizeLong();
    uint8_t olddirent_buf[olddirent_size];
    origin_dirent.SerializeToArray(olddirent_buf, olddirent_size);

    // and save it into the new directory entry
    {
      const auto key = pack_dentry_key(newparent, newname);
      fdb_transaction_set(transaction.get(), key.data(), key.size(),
                          olddirent_buf, olddirent_size);
    }
  }
#ifdef RENAME_EXCHANGE
  else if (flags == RENAME_EXCHANGE) {
    /**
     * This case is only slightly more complicated than
     * the previous case. Here we swap the contents of the
     * two directory entries, but nothing is unlinked.
     */
    const int olddirent_size = origin_dirent.ByteSizeLong();
    uint8_t olddirent_buf[olddirent_size];
    origin_dirent.SerializeToArray(olddirent_buf, olddirent_size);

    const int newdirent_size = destination_dirent.ByteSizeLong();
    uint8_t newdirent_buf[newdirent_size];
    destination_dirent.SerializeToArray(newdirent_buf, newdirent_size);

    {
      const auto key = pack_dentry_key(oldparent, oldname);
      fdb_transaction_set(transaction.get(), key.data(), key.size(),
                          newdirent_buf, newdirent_size);
    }

    {
      const auto key = pack_dentry_key(newparent, newname);
      fdb_transaction_set(transaction.get(), key.data(), key.size(),
                          olddirent_buf, olddirent_size);
    }
  }
#endif
  else if (flags == 0) {
    /**
     * This is the hard case. We're moving the origin
     * over top of an existing destination.
     * Since there's something at the destination, we'll
     * have to get rid of it.
     * TODO ugh can we share this code with unlink/rmdir?
     **/
    if (destination_dirent.type() == ft_directory) {
      /**
       * The destination is a directory. We'll need to know
       * if it is empty before we can remove it.
       */
      const auto key_start = pack_dentry_key(destination_dirent.inode(), "");
      const auto key_stop = pack_dentry_key(destination_dirent.inode(), "\xff");

      wait_on_future(fdb_transaction_get_range(
                         transaction.get(), key_start.data(), key_start.size(),
                         0, 1, key_stop.data(), key_stop.size(), 0, 1, 1, 0,
                         FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
                     directory_listing_fetch);
    }

    /**
     * Regardless of what the destination is, we need to
     * fetch its inode and use records.
     */
    const auto key_start = pack_inode_key(destination_dirent.inode());
    auto key_stop = pack_inode_key(destination_dirent.inode());
    // this ensures we cover the use records, located at \x01
    key_stop.push_back('\x02');

    wait_on_future(fdb_transaction_get_range(
                       transaction.get(), key_start.data(), key_start.size(), 0,
                       1, key_stop.data(), key_stop.size(), 0, 1,
                       // we don't care how many use
                       // records there are, we just
                       // need to know if there are
                       // 0, or >0. so, limit=2
                       2, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0),
                   inode_metadata_fetch);
    return InflightAction::BeginWait(
        std::bind(&Inflight_rename::complicated, this));
  } else {
    return InflightAction::Abort(ENOSYS);
  }

  /**
   * If we've made it here, then we were in a simple case, and
   * we're all done except for the commit. So schedule that,
   * and head off to the commit callback when it finishes.
   */
  return commit(InflightAction::OK);
}

InflightCallback Inflight_rename::issue() {
  {
    const auto key = pack_inode_key(oldparent);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        oldparent_inode_lookup);
  }

  {
    const auto key = pack_inode_key(newparent);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        newparent_inode_lookup);
  }

  {
    const auto key = pack_dentry_key(oldparent, oldname);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        origin_lookup);
  }

  {
    const auto key = pack_dentry_key(newparent, newname);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        destination_lookup);
  }

  // TODO probably also need to fetch information about the parent inodes
  // for permissions checking.

  return std::bind(&Inflight_rename::check, this);
}

extern "C" void fdbfs_rename(fuse_req_t req, fuse_ino_t parent,
                             const char *name, fuse_ino_t newparent,
                             const char *newname, unsigned int flags) {
  if (filename_length_check(req, name) || filename_length_check(req, newname)) {
    return;
  }
  Inflight_rename *inflight =
      new Inflight_rename(req, parent, std::string(name), newparent,
                          std::string(newname), flags, make_transaction());
  inflight->start();
}
