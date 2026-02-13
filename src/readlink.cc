
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fdbfs_ops.h"
#include "inflight.h"
#include "util.h"

/*************************************************************
 * readlink
 *************************************************************
 * INITIAL PLAN
 * this should be a single get, as we only store symlinks in
 * the inode. we _could_ fall back to storing very large symlinks
 * in the data blocks.
 *
 * REAL PLAN
 * ???
 */
struct AttemptState_readlink : public AttemptState {
  unique_future inode_fetch;
};

class Inflight_readlink
    : public InflightWithAttempt<AttemptState_readlink> {
public:
  Inflight_readlink(fuse_req_t, fuse_ino_t, unique_transaction);
  InflightCallback issue();

private:
  const fuse_ino_t ino;
  InflightAction callback();
};

Inflight_readlink::Inflight_readlink(fuse_req_t req, fuse_ino_t ino,
                                     unique_transaction transaction)
    : InflightWithAttempt(req, ReadWrite::ReadOnly, std::move(transaction)),
      ino(ino) {}

InflightAction Inflight_readlink::callback() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(a().inode_fetch.get(), &present, &val, &vallen);
  if (err)
    return InflightAction::FDBError(err);

  if (present) {
    INodeRecord inode;
    inode.ParseFromArray(val, vallen);
    if (!inode.has_symlink()) {
      if (inode.type() == ft_symlink) {
        return InflightAction::Abort(EIO);
      } else {
        return InflightAction::Abort(EINVAL);
      }
    }
    return InflightAction::Readlink(inode.symlink());
  } else {
    return InflightAction::Abort(ENOENT);
  }
}

InflightCallback Inflight_readlink::issue() {
  const auto key = pack_inode_key(ino);

  // and request just that inode
  wait_on_future(
      fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
      a().inode_fetch);
  return std::bind(&Inflight_readlink::callback, this);
}

extern "C" void fdbfs_readlink(fuse_req_t req, fuse_ino_t ino) {
  Inflight_readlink *inflight =
      new Inflight_readlink(req, ino, make_transaction());
  inflight->start();
}
