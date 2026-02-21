
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <limits>

#include "fdbfs_ops.h"
#include "inflight.h"
#include "util.h"
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
struct AttemptState_mknod : public AttemptState {
  unique_future dirinode_fetch;
  unique_future inode_check;
  unique_future dirent_check;
  fuse_ino_t ino = 0;
  struct stat attr{};
};

class Inflight_mknod : public InflightWithAttempt<AttemptState_mknod> {
public:
  Inflight_mknod(fuse_req_t, fuse_ino_t, std::string, mode_t, filetype, dev_t,
                 unique_transaction, std::optional<std::string>);
  InflightCallback issue();

private:
  const fuse_ino_t parent;
  const std::string name;
  const filetype type;
  const mode_t mode;
  const dev_t rdev;
  const std::string symlink_target;

  InflightAction postverification();
  InflightAction oplog_recovery(const OpLogRecord &) override;
};

Inflight_mknod::Inflight_mknod(
    fuse_req_t req, fuse_ino_t parent, std::string name, mode_t mode,
    filetype type, dev_t rdev, unique_transaction transaction,
    std::optional<std::string> symlink_target_opt = std::nullopt)
    : InflightWithAttempt(req, ReadWrite::Yes, std::move(transaction)),
      parent(parent), name(std::move(name)), type(type), mode(mode), rdev(rdev),
      symlink_target((type == ft_symlink && symlink_target_opt.has_value())
                         ? std::move(*symlink_target_opt)
                         : std::string()) {}

InflightAction Inflight_mknod::postverification() {
  fdb_bool_t dirinode_present, inode_present, dirent_present;
  const uint8_t *value;
  int valuelen;
  fdb_error_t err;

  err = fdb_future_get_value(a().dirinode_fetch.get(), &dirinode_present,
                             &value, &valuelen);
  if (err)
    return InflightAction::FDBError(err);

  if (!dirinode_present) {
    // the parent directory doesn't exist
    return InflightAction::Abort(ENOENT);
  }

  INodeRecord parentinode;
  parentinode.ParseFromArray(value, valuelen);
  if (!(parentinode.IsInitialized() && parentinode.has_nlinks())) {
    return InflightAction::Abort(EIO);
  }

  err = fdb_future_get_value(a().dirent_check.get(), &dirent_present, &value,
                             &valuelen);
  if (err)
    return InflightAction::FDBError(err);

  err = fdb_future_get_value(a().inode_check.get(), &inode_present, &value,
                             &valuelen);
  if (err)
    return InflightAction::FDBError(err);

  if (dirent_present) {
    // can't make this entry, there's already something there
    return InflightAction::Abort(EEXIST);
  }

  if (inode_present) {
    // astonishingly we guessed an inode that already exists.
    // try this again!
    return InflightAction::Restart();
  }

  // TODO we need to fetch the parent inode for permissions checking
  if (parentinode.nlinks() <= 1) {
    // directory is unlinked, no new entries to be created
    return InflightAction::Abort(ENOENT);
  }

  if (type == ft_directory) {
    if (parentinode.nlinks() == std::numeric_limits<uint64_t>::max()) {
      return InflightAction::Abort(EMLINK);
    }
    parentinode.set_nlinks(parentinode.nlinks() + 1);
  }

  // update the containing directory entry
  update_directory_times(transaction.get(), parentinode);

  INodeRecord inode;
  inode.set_inode(a().ino);
  inode.set_type(type);
  if (type == ft_symlink)
    inode.set_symlink(symlink_target);
  inode.set_mode(mode);
  inode.set_nlinks((type == ft_directory) ? 2 : 1);
  if (type == ft_symlink)
    inode.set_size(symlink_target.size());
  else
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
  pack_inode_record_into_stat(inode, a().attr);

  // set the inode KV pair
  if (!fdb_set_protobuf(transaction.get(), pack_inode_key(a().ino), inode))
    return InflightAction::Abort(EIO);

  DirectoryEntry dirent;
  dirent.set_inode(a().ino);
  dirent.set_type(type);

  if (!fdb_set_protobuf(transaction.get(), pack_dentry_key(parent, name),
                        dirent))
    return InflightAction::Abort(EIO);

  OpLogResultEntry result_entry;
  result_entry.set_ino(a().ino);
  result_entry.set_generation(1);
  *result_entry.mutable_attr() = pack_stat_into_stat_record(a().attr);
  result_entry.set_attr_timeout(0.01);
  result_entry.set_entry_timeout(0.01);
  if (!write_oplog_result(result_entry)) {
    return InflightAction::Abort(EIO);
  }

  return commit([&]() {
    struct fuse_entry_param e{};
    e.ino = a().ino;
    e.generation = 1;
    e.attr = a().attr;
    e.attr_timeout = 0.01;
    e.entry_timeout = 0.01;
    return InflightAction::Entry(e);
  });
}

InflightAction Inflight_mknod::oplog_recovery(const OpLogRecord &record) {
  if (record.result_case() != OpLogRecord::kEntry) {
    return InflightAction::Abort(EIO);
  }
  if (!record.entry().has_attr()) {
    return InflightAction::Abort(EIO);
  }

  struct fuse_entry_param e{};
  e.ino = record.entry().ino();
  e.generation = record.entry().generation();
  unpack_stat_record_into_stat(record.entry().attr(), e.attr);
  e.attr_timeout = record.entry().attr_timeout();
  e.entry_timeout = record.entry().entry_timeout();
  return InflightAction::Entry(e);
}

InflightCallback Inflight_mknod::issue() {
  a().ino = 0x47d8d31b9848016f;
  do {
    if (getrandom(reinterpret_cast<void *>(&a().ino), sizeof(a().ino),
                  GRND_NONBLOCK) < static_cast<ssize_t>(sizeof(a().ino))) {
      // didn't get as many bytes as we wanted.
      struct timespec ts;
      if (clock_gettime(CLOCK_MONOTONIC, &ts) == -1) {
        // we are just having a bad time today. we're going to
        // have to fail, we don't have any way of making plausibly
        // random inode numbers at the moment.
        return []() { return InflightAction::Abort(EIO); };
      }
      // take whatever was laying around in ino after the getrandom call
      // and xor it with the current nanoseconds. it isn't random, but it'll
      // at least be somewhat distributed over the space.
      a().ino ^= ts.tv_nsec;
    }
  } while (a().ino < 128); // treat the first 128 as reserved

  {
    const auto key = pack_inode_key(parent);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        a().dirinode_fetch);
  }

  {
    const auto key = pack_dentry_key(parent, name);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        a().dirent_check);
  }

  {
    const auto key = pack_inode_key(a().ino);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        a().inode_check);
  }

  return std::bind(&Inflight_mknod::postverification, this);
}

extern "C" void fdbfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
                            mode_t mode, dev_t rdev) {
  if (filename_length_check(req, name)) {
    return;
  }
  filetype deduced_type;
  // validate mode value
  switch (mode & S_IFMT) {
  case S_IFSOCK:
    deduced_type = ft_socket;
    break;
  case S_IFLNK:
    deduced_type = ft_symlink;
    break;
  case S_IFREG:
    deduced_type = ft_regular;
    break;
  case S_IFCHR:
    deduced_type = ft_character;
    break;
  case S_IFIFO:
    deduced_type = ft_fifo;
    break;
  default: {
    // unsupported value. abort.
    fuse_reply_err(req, EPERM);
    return;
  }
  }

  Inflight_mknod *inflight =
      new Inflight_mknod(req, parent, name, mode & (~S_IFMT), deduced_type,
                         rdev, make_transaction());
  inflight->start();
}

extern "C" void fdbfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                            mode_t mode) {
  if (filename_length_check(req, name)) {
    return;
  }
  Inflight_mknod *inflight = new Inflight_mknod(
      req, parent, name, mode & (~S_IFMT), ft_directory, 0, make_transaction());
  inflight->start();
}

extern "C" void fdbfs_symlink(fuse_req_t req, const char *target,
                              fuse_ino_t parent, const char *name) {
  // TODO eliminate magic number for symlink target length
  if (filename_length_check(req, target, 1024) ||
      filename_length_check(req, name)) {
    return;
  }
  Inflight_mknod *inflight =
      new Inflight_mknod(req, parent, name, 0777 & (~S_IFMT), ft_symlink, 0,
                         make_transaction(), std::string(target));
  inflight->start();
}
