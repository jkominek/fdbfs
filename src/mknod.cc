
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
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
#include "fdbfs_ops.h"

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
		 filetype, dev_t, unique_transaction,
                 std::optional<std::string>);
  Inflight_mknod *reincarnate();
  InflightCallback issue();
private:
  unique_future dirinode_fetch;
  unique_future inode_check;
  unique_future dirent_check;

  fuse_ino_t parent;
  fuse_ino_t ino;
  struct stat attr {};
  std::string name;
  filetype type;
  mode_t mode;
  dev_t rdev;
  std::string symlink_target;

  InflightAction postverification();
};

Inflight_mknod::Inflight_mknod(fuse_req_t req, fuse_ino_t parent,
			       std::string name, mode_t mode,
			       filetype type, dev_t rdev,
			       unique_transaction transaction,
                               std::optional<std::string> symlink_target = std::nullopt)
  : Inflight(req, ReadWrite::Yes, std::move(transaction)),
    parent(parent), name(name), type(type), mode(mode), rdev(rdev)
{
  if(type == ft_symlink)
    this->symlink_target = symlink_target.value();
}

Inflight_mknod *Inflight_mknod::reincarnate()
{
  Inflight_mknod *x = new Inflight_mknod(req, parent, name, mode,
					 type, rdev,
					 std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_mknod::postverification()
{
  fdb_bool_t dirinode_present, inode_present, dirent_present;
  const uint8_t *value; int valuelen;
  fdb_error_t err;

  err = fdb_future_get_value(dirinode_fetch.get(), &dirinode_present, &value, &valuelen);
  if(err) return InflightAction::FDBError(err);

  if(!dirinode_present) {
    // the parent directory doesn't exist
    return InflightAction::Abort(ENOENT);
  }

  INodeRecord parentinode;
  parentinode.ParseFromArray(value, valuelen);

  err = fdb_future_get_value(dirent_check.get(), &dirent_present, &value, &valuelen);
  if(err) return InflightAction::FDBError(err);

  err = fdb_future_get_value(inode_check.get(), &inode_present, &value, &valuelen);
  if(err) return InflightAction::FDBError(err);

  if(dirent_present) {
    // can't make this entry, there's already something there
    return InflightAction::Abort(EEXIST);
  }

  if(inode_present) {
    // astonishingly we guessed an inode that already exists.
    // try this again!
    return InflightAction::Restart();
  }

  // TODO we need to fetch the parent inode for permissions checking
  if(parentinode.nlinks() <= 1) {
    // directory is unlinked, no new entries to be created
    return InflightAction::Abort(ENOENT);
  }

  // update the containing directory entry
  update_directory_times(transaction.get(), parentinode);

  INodeRecord inode;
  inode.set_inode(ino);
  inode.set_type(type);
  if(type == ft_symlink)
    inode.set_symlink(symlink_target);
  inode.set_mode(mode);
  inode.set_nlinks((type == ft_directory) ? 2 : 1);
  inode.set_size(0);
  inode.set_rdev(rdev);
  const fuse_ctx *ctx = fuse_req_ctx(req);
  inode.set_uid(ctx->uid);
  inode.set_gid(ctx->gid);

  struct timespec tp;
  clock_gettime(CLOCK_REALTIME, &tp);
  inode.mutable_atime()->set_sec(tp.tv_sec);
  inode.mutable_atime()->set_nsec(tp.tv_nsec);
  inode.mutable_mtime()->set_sec(tp.tv_sec);
  inode.mutable_mtime()->set_nsec(tp.tv_nsec);
  inode.mutable_ctime()->set_sec(tp.tv_sec);
  inode.mutable_ctime()->set_nsec(tp.tv_nsec);

  // wrap it up to be returned to fuse later
  pack_inode_record_into_stat(inode, attr);
  
  // set the inode KV pair
  auto key = pack_inode_key(ino);
  int inode_size = inode.ByteSizeLong();
  uint8_t inode_buffer[inode_size];
  inode.SerializeToArray(inode_buffer, inode_size);

  fdb_transaction_set(transaction.get(),
		      key.data(), key.size(),
		      inode_buffer, inode_size);
  
  DirectoryEntry dirent;
  dirent.set_inode(ino);
  dirent.set_type(type);

  int dirent_size = dirent.ByteSizeLong();
  uint8_t dirent_buffer[dirent_size];
  dirent.SerializeToArray(dirent_buffer, dirent_size);

  key = pack_dentry_key(parent, name);
  fdb_transaction_set(transaction.get(),
		      key.data(), key.size(),
		      dirent_buffer, dirent_size);

  return commit([&]() {
    auto e = std::make_unique<struct fuse_entry_param>();
    bzero(e.get(), sizeof(struct fuse_entry_param));
    e->ino = ino;
    e->generation = 1;
    e->attr = attr;
    e->attr_timeout = 0.01;
    e->entry_timeout = 0.01;
    return InflightAction::Entry(std::move(e));
  });
}

InflightCallback Inflight_mknod::issue()
{
  ino = generate_inode();

  {
    const auto key = pack_inode_key(parent);
    wait_on_future(fdb_transaction_get(transaction.get(),
                                       key.data(), key.size(), 0),
                   dirinode_fetch);
  }

  {
    const auto key = pack_dentry_key(parent, name);
    wait_on_future(fdb_transaction_get(transaction.get(),
                                       key.data(), key.size(), 0),
                   dirent_check);
  }

  {
    const auto key = pack_inode_key(ino);
    wait_on_future(fdb_transaction_get(transaction.get(),
                                       key.data(), key.size(), 0),
                   inode_check);
  }

  return std::bind(&Inflight_mknod::postverification, this);
}

extern "C" void fdbfs_mknod(fuse_req_t req, fuse_ino_t parent,
			    const char *name, mode_t mode,
			    dev_t rdev)
{
  if(filename_length_check(req, name)) {
    return;
  }
  filetype deduced_type;
  // validate mode value
  switch(mode & S_IFMT) {
  case S_IFSOCK: deduced_type = ft_socket; break;
  case S_IFLNK:  deduced_type = ft_symlink; break;
  case S_IFREG:  deduced_type = ft_regular; break;
  case S_IFCHR:  deduced_type = ft_character; break;
  case S_IFIFO:  deduced_type = ft_fifo; break;
  default: {
    // unsupported value. abort.
    fuse_reply_err(req, EPERM);
    return;
  }
  }

  Inflight_mknod *inflight =
    new Inflight_mknod(req, parent, name, mode & (~S_IFMT),
		       deduced_type, rdev, make_transaction());
  inflight->start();
}

extern "C" void fdbfs_mkdir(fuse_req_t req, fuse_ino_t parent,
			    const char *name, mode_t mode)
{
  if(filename_length_check(req, name)) {
    return;
  }
  Inflight_mknod *inflight =
    new Inflight_mknod(req, parent, name, mode & (~S_IFMT),
		       ft_directory, 0, make_transaction());
  inflight->start();
}

extern "C" void fdbfs_symlink(fuse_req_t req, const char *target,
                              fuse_ino_t parent, const char *name)
{
  if(filename_length_check(req, target, 1024) ||
     filename_length_check(req, name)) {
    return;
  }
  Inflight_mknod *inflight =
    new Inflight_mknod(req, parent, name, 0777 & (~S_IFMT),
                       ft_symlink, 0, make_transaction(),
                       std::string(target));
  inflight->start();
}
