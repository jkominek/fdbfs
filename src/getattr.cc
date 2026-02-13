
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

#include <iostream>

#include "fdbfs_ops.h"
#include "inflight.h"
#include "util.h"

#include <cstdint>
#include <typeinfo>

/*************************************************************
 * getattr
 *************************************************************
 * INITIAL PLAN
 * this should be a single get.
 *
 * REAL PLAN
 * maybe a small range read to pick up extended attributes
 * or less common values?
 */
struct AttemptState_getattr : public AttemptState {
  unique_future inode_fetch;
};

class Inflight_getattr : public InflightWithAttempt<AttemptState_getattr> {
public:
  Inflight_getattr(fuse_req_t, fuse_ino_t, unique_transaction);
  InflightCallback issue();

private:
  const fuse_ino_t ino;
  InflightAction callback();
};

Inflight_getattr::Inflight_getattr(fuse_req_t req, fuse_ino_t ino,
                                   unique_transaction transaction)
    : InflightWithAttempt(req, ReadWrite::ReadOnly, std::move(transaction)),
      ino(ino) {}

InflightAction Inflight_getattr::callback() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;
  err = fdb_future_get_value(a().inode_fetch.get(), &present, &val, &vallen);
  if (err)
    return InflightAction::FDBError(err);
  if (!present) {
    return InflightAction::Abort(ENOENT);
  }

  INodeRecord inode;
  inode.ParseFromArray(val, vallen);
  if (!inode.IsInitialized()) {
    return InflightAction::Abort(EIO);
  }

  struct stat attr{};
  pack_inode_record_into_stat(inode, attr);
  return InflightAction::Attr(attr);
}

InflightCallback Inflight_getattr::issue() {
  auto key = pack_inode_key(ino);

  // and request just that inode
  wait_on_future(
      fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
      a().inode_fetch);
  return std::bind(&Inflight_getattr::callback, this);
}

extern "C" void fdbfs_getattr(fuse_req_t req, fuse_ino_t ino,
                              struct fuse_file_info *fi) {
  // get the file attributes of an inode
  Inflight_getattr *inflight =
      new Inflight_getattr(req, ino, make_transaction());
  inflight->start();
}
