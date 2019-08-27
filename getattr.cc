
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

#include <iostream>

#include "util.h"
#include "inflight.h"

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
class Inflight_getattr : public Inflight {
public:
  Inflight_getattr(fuse_req_t, fuse_ino_t, FDBTransaction * = NULL);
  void issue();
  Inflight_getattr *reincarnate();
private:
  fuse_ino_t ino;

  unique_future inode_fetch;
  InflightAction callback();
};

Inflight_getattr::Inflight_getattr(fuse_req_t req, fuse_ino_t ino, FDBTransaction *transaction)
  : Inflight(req, false, transaction), ino(ino)
{
}

Inflight_getattr *Inflight_getattr::reincarnate()
{
  Inflight_getattr *x = new Inflight_getattr(req, ino, transaction.release());
  delete this;
  return x;
}

InflightAction Inflight_getattr::callback()
{
  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  if(fdb_future_get_value(inode_fetch.get(), &present, (const uint8_t **)&val, &vallen)) {
    // try again?
    return InflightAction::Restart();
  }

  if(!present) {
    return InflightAction::Abort(EFAULT);
  }

  INodeRecord inode;
  inode.ParseFromArray(val, vallen);
  if(!inode.IsInitialized()) {
    return InflightAction::Abort(EIO);
  }

  auto attr = std::make_unique<struct stat>();
  pack_inode_record_into_stat(&inode, attr.get());
  return InflightAction::Attr(std::move(attr));
}

void Inflight_getattr::issue()
{
  auto key = pack_inode_key(ino);

  // and request just that inode
  FDBFuture *f = fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0);
  wait_on_future(f, &inode_fetch);
  cb.emplace(std::bind(&Inflight_getattr::callback, this));
  //std::cout << type_name<decltype(std::bind(&Inflight_getattr::callback, this))>() << std::endl;
}

extern "C" void fdbfs_getattr(fuse_req_t req, fuse_ino_t ino,
			      struct fuse_file_info *fi)
{
  // get the file attributes of an inode
  Inflight_getattr *inflight = new Inflight_getattr(req, ino);
  inflight->start();
}
