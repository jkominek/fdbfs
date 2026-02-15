#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fdbfs_ops.h"
#include "inflight.h"
#include "util.h"
#include "values.pb.h"

/*************************************************************
 * lookup
 *************************************************************
 * INITIAL PLAN
 * not too bad. given a directory inode and a name, we:
 *   1. fetch the directory entry, getting us an inode.
 *   2. getattr-equiv on that inode.
 *
 * REAL PLAN
 * might be a good spot for an optimization? we can finish off
 * the fuse request, but then maintain some inode cache with
 * the last unchanging attributes of an inode that we've seen.
 * we could reject invalid requests to inodes faster. (readdir
 * on a file, for instance?) is it worth it to make the error
 * case faster?
 *
 * TRANSACTIONAL BEHAVIOR
 * We're doing the two reads as snapshots. Since the filesystem
 * can change arbitrarily immediately after we're done, it doesn't
 * much matter if it changes by a little or a lot. Just want to
 * ensure that we show the user something that was true.
 */
struct AttemptState_lookup : public AttemptState {
  fuse_ino_t target = 0;
  unique_future dirent_fetch;
  unique_future inode_fetch;
};

class Inflight_lookup : public InflightWithAttempt<AttemptState_lookup> {
public:
  Inflight_lookup(fuse_req_t, fuse_ino_t, std::string, unique_transaction);
  InflightCallback issue();

private:
  const fuse_ino_t parent;
  const std::string name;

  // issue looks up the dirent and then...
  InflightAction lookup_inode();
  InflightAction process_inode();
};

Inflight_lookup::Inflight_lookup(fuse_req_t req, fuse_ino_t parent,
                                 std::string name,
                                 unique_transaction transaction)
    : InflightWithAttempt(req, ReadWrite::Yes, std::move(transaction)),
      parent(parent), name(std::move(name)) {}

InflightAction Inflight_lookup::process_inode() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(a().inode_fetch.get(), &present, &val, &vallen);
  if (err)
    return InflightAction::FDBError(err);

  // and second callback, to get the attributes
  if (!present) {
    return InflightAction::Abort(EIO);
  }

  INodeRecord inode;
  inode.ParseFromArray(val, vallen);
  if (!inode.IsInitialized()) {
    return InflightAction::Abort(EIO);
  }

  struct fuse_entry_param e{};
  e.ino = a().target;
  // TODO technically we need to be smarter about generations
  e.generation = 1;
  pack_inode_record_into_stat(inode, e.attr);
  e.attr_timeout = 0.01;
  e.entry_timeout = 0.01;

  return InflightAction::Entry(e);
}

InflightAction Inflight_lookup::lookup_inode() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(a().dirent_fetch.get(), &present, &val, &vallen);
  if (err)
    return InflightAction::FDBError(err);

  // we're on the first callback, to get the directory entry
  if (present) {
    {
      DirectoryEntry dirent;
      dirent.ParseFromArray(val, vallen);
      if (!dirent.has_inode()) {
        return InflightAction::Abort(EIO, "directory entry missing inode");
      }
      a().target = dirent.inode();
    }

    const auto key = pack_inode_key(a().target);

    // and request just that inode
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 1),
        a().inode_fetch);
    return InflightAction::BeginWait(
        std::bind(&Inflight_lookup::process_inode, this));
  } else {
    return InflightAction::Abort(ENOENT);
  }
}

InflightCallback Inflight_lookup::issue() {
  const auto key = pack_dentry_key(parent, name);

  wait_on_future(
      fdb_transaction_get(transaction.get(), key.data(), key.size(), 1),
      a().dirent_fetch);
  return std::bind(&Inflight_lookup::lookup_inode, this);
}

extern "C" void fdbfs_lookup(fuse_req_t req, fuse_ino_t parent,
                             const char *name) {
  if (filename_length_check(req, name)) {
    return;
  }
  std::string sname(name);
  Inflight_lookup *inflight =
      new Inflight_lookup(req, parent, sname, make_transaction());
  inflight->start();
}
