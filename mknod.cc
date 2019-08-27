
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
 * mknod
 *************************************************************
 * INITIAL PLAN
 * make up a random inode number. check to see if it is allocated.
 * check to see if there is an existing dirent for the name in
 * question. if they're both empty, proceed to make the two
 * records.
 *
 * REAL PLAN
 * ???
 */
class Inflight_mknod : public Inflight {
public:
  Inflight_mknod(fuse_req_t, fuse_ino_t, std::string, mode_t,
		 filetype, dev_t, FDBTransaction * = 0);
  Inflight_mknod *reincarnate();
  void issue();
private:
  unique_future inode_check;
  unique_future dirent_check;
  unique_future commit;
  fuse_ino_t parent;
  fuse_ino_t ino;
  struct stat attr;
  std::string name;
  filetype type;
  mode_t mode;
  dev_t rdev;

  InflightAction commit_cb();
  InflightAction postverification();
};

Inflight_mknod::Inflight_mknod(fuse_req_t req, fuse_ino_t parent,
			       std::string name, mode_t mode,
			       filetype type, dev_t rdev,
			       FDBTransaction *transaction)
  : Inflight(req, true, transaction), parent(parent), name(name),
    type(type), mode(mode), rdev(rdev)
{
}

Inflight_mknod *Inflight_mknod::reincarnate()
{
  Inflight_mknod *x = new Inflight_mknod(req, parent, name, mode,
					 type, rdev,
					 transaction.release());
  delete this;
  return x;
}

InflightAction Inflight_mknod::commit_cb()
{
  auto e = std::make_unique<struct fuse_entry_param>();
  bzero(e.get(), sizeof(struct fuse_entry_param));
  e->ino = ino;
  e->generation = 1;
  bcopy(&(attr), &(e->attr), sizeof(struct stat));
  e->attr_timeout = 0.01;
  e->entry_timeout = 0.01;
  return InflightAction::Entry(std::move(e));
}

InflightAction Inflight_mknod::postverification()
{
  fdb_bool_t inode_present, dirent_present;
  const uint8_t *value; int valuelen;
  if(fdb_future_get_value(dirent_check.get(), &dirent_present,
			  &value, &valuelen)) {
    return InflightAction::Restart();
  }
  if(fdb_future_get_value(inode_check.get(), &inode_present,
			  &value, &valuelen)) {
    return InflightAction::Restart();
  }
  
  if(dirent_present) {
    // can't make this entry, there's already something there
    return InflightAction::Abort(EEXIST);
  }

  if(inode_present) {
    // astonishingly we guessed an inode that already exists.
    // try this again!
    return InflightAction::Restart();
  }

  INodeRecord inode;
  inode.set_inode(ino);
  inode.set_type(type);
  inode.set_mode(mode);
  inode.set_nlinks((type == directory) ? 2 : 1);
  inode.set_size(0);
  inode.set_rdev(rdev);
  inode.set_uid(0);
  inode.set_uid(0);

  inode.mutable_atime()->set_sec(1565989127);
  inode.mutable_atime()->set_nsec(0);

  inode.mutable_mtime()->set_sec(1565989127);
  inode.mutable_mtime()->set_nsec(0);

  inode.mutable_ctime()->set_sec(1565989127);
  inode.mutable_ctime()->set_nsec(0);

  // wrap it up to be returned to fuse later
  pack_inode_record_into_stat(&inode, &(attr));
  
  // set the inode KV pair
  auto key = pack_inode_key(ino);
  int inode_size = inode.ByteSize();
  uint8_t inode_buffer[inode_size];
  inode.SerializeToArray(inode_buffer, inode_size);
  
  fdb_transaction_set(transaction.get(),
		      key.data(), key.size(),
		      inode_buffer, inode_size);
  
  DirectoryEntry dirent;
  dirent.set_inode(ino);
  dirent.set_type(type);

  int dirent_size = dirent.ByteSize();
  uint8_t dirent_buffer[dirent_size];
  dirent.SerializeToArray(dirent_buffer, dirent_size);

  key = pack_dentry_key(parent, name);
  fdb_transaction_set(transaction.get(),
		      key.data(), key.size(),
		      dirent_buffer, dirent_size);

  wait_on_future(fdb_transaction_commit(transaction.get()),
		 &commit);
  cb.emplace(std::bind(&Inflight_mknod::commit_cb, this));

  return InflightAction::BeginWait();
}

void Inflight_mknod::issue()
{
  ino = generate_inode();

  auto key = pack_dentry_key(parent, name);
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &dirent_check);
  
  key = pack_inode_key(ino);
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &inode_check);

  cb.emplace(std::bind(&Inflight_mknod::postverification, this));
}

extern "C" void fdbfs_mknod(fuse_req_t req, fuse_ino_t parent,
			    const char *name, mode_t mode,
			    dev_t rdev)
{
  filetype deduced_type;
  // validate mode value
  switch(mode & S_IFMT) {
  case S_IFSOCK: deduced_type = socket; break;
  case S_IFLNK:  deduced_type = symlink; break;
  case S_IFREG:  deduced_type = regular; break;
  case S_IFCHR:  deduced_type = character; break;
  case S_IFIFO:  deduced_type = fifo; break;
  default: {
    // unsupported value. abort.
    fuse_reply_err(req, EPERM);
    return;
  }
  }

  Inflight_mknod *inflight = new Inflight_mknod(req, parent, name,
						mode & (~S_IFMT),
						deduced_type,
						rdev);
  inflight->start();
}

extern "C" void fdbfs_mkdir(fuse_req_t req, fuse_ino_t parent,
			    const char *name, filetype type,
			    mode_t mode)
{
  Inflight_mknod *inflight = new Inflight_mknod(req, parent, name,
						mode & (~S_IFMT),
						directory, 0);
  inflight->start();
}
