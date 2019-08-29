
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
 * symlink
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
class Inflight_symlink : public Inflight {
public:
  Inflight_symlink(fuse_req_t, std::string, fuse_ino_t, std::string,
		   unique_transaction transaction);
  Inflight_symlink *reincarnate();
  InflightCallback issue();
private:
  unique_future inode_check;
  unique_future dirent_check;
  unique_future commit;
  std::string link;
  fuse_ino_t parent;
  std::string name;
  fuse_ino_t ino;
  struct stat attr;

  InflightAction postverification();
  InflightAction commit_cb();
};

Inflight_symlink::Inflight_symlink(fuse_req_t req, std::string link,
				   fuse_ino_t parent, std::string name,
				   unique_transaction transaction)
  : Inflight(req, true, std::move(transaction)),
    link(link), parent(parent), name(name)
{
}

Inflight_symlink *Inflight_symlink::reincarnate()
{
  Inflight_symlink *x = new Inflight_symlink(req, link, parent, name,
					     std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_symlink::commit_cb()
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

InflightAction Inflight_symlink::postverification()
{
  fdb_bool_t inode_present, dirent_present;
  const uint8_t *value; int valuelen;
  if(fdb_future_get_value(dirent_check.get(),
			  &dirent_present, &value, &valuelen) ||
     fdb_future_get_value(inode_check.get(),
			  &inode_present, &value, &valuelen)) {
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
  inode.set_type(symlink);
  inode.set_nlinks(1);
  inode.set_symlink(link);

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
  dirent.set_type(symlink);

  int dirent_size = dirent.ByteSize();
  uint8_t dirent_buffer[dirent_size];
  dirent.SerializeToArray(dirent_buffer, dirent_size);

  key = pack_dentry_key(parent, name);
  fdb_transaction_set(transaction.get(),
		      key.data(), key.size(),
		      dirent_buffer, dirent_size);

  wait_on_future(fdb_transaction_commit(transaction.get()),
		 &commit);

  return InflightAction::BeginWait(std::bind(&Inflight_symlink::commit_cb, this));
}

InflightCallback Inflight_symlink::issue()
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

  return std::bind(&Inflight_symlink::postverification, this);
}

extern "C" void fdbfs_symlink(fuse_req_t req, const char *link,
			      fuse_ino_t parent, const char *name)
{
  Inflight_symlink *inflight =
    new Inflight_symlink(req, std::string(link),
			 parent, std::string(name), make_transaction());
  inflight->start();
}
