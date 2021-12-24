#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>

#include "util.h"
#include "inflight.h"
#include "values.pb.h"
#include "fdbfs_ops.h"

/*************************************************************
 * link
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
class Inflight_link : public Inflight {
public:
  Inflight_link(fuse_req_t, fuse_ino_t, fuse_ino_t, std::string,
		unique_transaction);
  Inflight_link *reincarnate();
  InflightCallback issue();
private:
  fuse_ino_t ino;
  fuse_ino_t newparent;
  std::string newname;

  INodeRecord inode;

  InflightAction check();
  InflightAction commit_cb();
  
  // is the file to link a non-directory?
  unique_future file_lookup;
  // is the destination location a directory?
  unique_future dir_lookup;
  // does the destination location already exist?
  unique_future target_lookup;

  unique_future commit;
};

Inflight_link::Inflight_link(fuse_req_t req, fuse_ino_t ino,
			     fuse_ino_t newparent, std::string newname,
			     unique_transaction transaction)
  : Inflight(req, true, std::move(transaction)), ino(ino),
    newparent(newparent), newname(newname)
{
}

Inflight_link *Inflight_link::reincarnate()
{
  Inflight_link *x = new Inflight_link(req, ino, newparent, newname,
				       std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_link::commit_cb()
{
  auto e = std::make_unique<struct fuse_entry_param>();
  bzero(e.get(), sizeof(struct fuse_entry_param));
  e->ino = ino;
  e->generation = 1;
  pack_inode_record_into_stat(&inode, &(e->attr));
  e->attr_timeout = 0.01;
  e->entry_timeout = 0.01;
  return InflightAction::Entry(std::move(e));
}

InflightAction Inflight_link::check()
{
  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  fdb_error_t err;
  
  // is the file a non-directory?
  err = fdb_future_get_value(file_lookup.get(), &present, (const uint8_t **)&val, &vallen);
  if(err)
    return InflightAction::FDBError(err);
  if(present) {
    inode.ParseFromArray(val, vallen);
    if(!inode.has_type()) {
      // error
      return InflightAction::Abort(EIO);
    } else if(inode.type() == directory) {
      // can hardlink anything except a directory
      return InflightAction::Abort(EPERM);
    }
    // we could lift this value and save it for the
    // other dirent we need to create?
  } else {
    // apparently it isn't there. sad.
    return InflightAction::Abort(ENOENT);
  }    

  // is the directory a directory?
  err = fdb_future_get_value(dir_lookup.get(), &present, (const uint8_t **)&val, &vallen);
  if(err)
    return InflightAction::FDBError(err);
  if(present) {
    INodeRecord dirinode;
    dirinode.ParseFromArray(val, vallen);
    if(!dirinode.has_type()) {
      return InflightAction::Abort(EIO);
      // error
    }
    if(dirinode.type() != directory) {
      // have to hardlink into a directory
      return InflightAction::Abort(ENOTDIR);
    }
    // update times on destination dir
    update_directory_times(transaction.get(), dirinode);
  } else {
    return InflightAction::Abort(ENOENT);
  }

  // Does the target exist?
  err = fdb_future_get_value(target_lookup.get(), &present, (const uint8_t **)&val, &vallen);
  if(err)
    return InflightAction::FDBError(err);
  if(present) {
    // that's an error. :(
    return InflightAction::Abort(EEXIST);
  }

  // need to update the inode attributes
  auto key = pack_inode_key(ino);
  inode.set_nlinks(inode.nlinks()+1);
  struct timespec tv;
  clock_gettime(CLOCK_REALTIME, &tv);
  update_ctime(&inode, &tv);
  // TODO do we need to touch any other inode attributes when
  // creating a hard link?
  int inode_size = inode.ByteSizeLong();
  uint8_t inode_buffer[inode_size];
  inode.SerializeToArray(inode_buffer, inode_size);
  fdb_transaction_set(transaction.get(),
		      key.data(), key.size(),
		      inode_buffer, inode_size);

  // also need to add the new directory entry
  DirectoryEntry dirent;
  dirent.set_inode(ino);
  dirent.set_type(inode.type());

  key = pack_dentry_key(newparent, newname);
  int dirent_size = dirent.ByteSizeLong();
  uint8_t dirent_buffer[dirent_size];
  dirent.SerializeToArray(dirent_buffer, dirent_size);
  fdb_transaction_set(transaction.get(),
		      key.data(), key.size(),
		      dirent_buffer, dirent_size);

  // commit
  wait_on_future(fdb_transaction_commit(transaction.get()), &commit);

  return InflightAction::BeginWait(std::bind(&Inflight_link::commit_cb, this));
}

InflightCallback Inflight_link::issue()
{
  // check that the file is a file
  auto key = pack_inode_key(ino);
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &file_lookup);

  // check/update destination directory
  key = pack_inode_key(newparent);
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &dir_lookup);

  // check nothing exists in the destination
  key = pack_dentry_key(newparent, newname);
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &target_lookup);

  return std::bind(&Inflight_link::check, this);
}

extern "C" void fdbfs_link(fuse_req_t req, fuse_ino_t ino,
			   fuse_ino_t newparent,
			   const char *newname)
{
  if(filename_length_check(req, newname)) {
    return;
  }
  Inflight_link *inflight =
    new Inflight_link(req, ino, newparent, std::string(newname),
		      make_transaction());
  inflight->start();
}
