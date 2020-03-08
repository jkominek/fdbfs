
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
#include <attr/xattr.h>

#include "util.h"
#include "inflight.h"
#include "fdbfs_ops.h"

/*************************************************************
 * getxattr
 *************************************************************
 * INITIAL PLAN
 * two stage get
 *
 * REAL PLAN
 * ???
 */
class Inflight_getxattr : public Inflight {
public:
  Inflight_getxattr(fuse_req_t, fuse_ino_t, std::string, size_t, unique_transaction);
  Inflight_getxattr *reincarnate();
  InflightCallback issue();
private:
  fuse_ino_t ino;
  std::string name;
  size_t maxsize;

  unique_future xattr_node_fetch;
  unique_future xattr_data_fetch;
  InflightAction process();
};

Inflight_getxattr::Inflight_getxattr(fuse_req_t req, fuse_ino_t ino,
				     std::string name, size_t maxsize,
				     unique_transaction transaction)
  : Inflight(req, false, std::move(transaction)),
    ino(ino), name(name), maxsize(maxsize)
{
}

Inflight_getxattr *Inflight_getxattr::reincarnate()
{
  Inflight_getxattr *x = new Inflight_getxattr(req, ino, name, maxsize,
					       std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_getxattr::process()
{
  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(xattr_node_fetch.get(), &present, (const uint8_t **)&val, &vallen);
  if(err) return InflightAction::FDBError(err);

  if(present) {
    // decode the xattr node to follow it to the id
    XAttrRecord xattr;
    xattr.ParseFromArray(val, vallen);
    if(!xattr.IsInitialized()) {
      return InflightAction::Abort(EIO);
    }

    err = fdb_future_get_value(xattr_data_fetch.get(), &present, (const uint8_t **)&val, &vallen);
    if(err) return InflightAction::FDBError(err);

    if(present) {
      // decode
      if(maxsize==0) {
	// just want the decoded size
	return InflightAction::XattrSize(vallen);
      } else {
	std::vector<uint8_t> buffer;
	buffer.assign(val, val+vallen);
	return InflightAction::Buf(buffer);
      }
    } else {
      if(maxsize==0) {
	return InflightAction::XattrSize(0);
      } else {
	std::vector<uint8_t> buffer;
	return InflightAction::Buf(buffer);
      }
    }
  } else {
    return InflightAction::Abort(ENOATTR);
  }
}

InflightCallback Inflight_getxattr::issue()
{
  // we could just not fetch the node, since it doesn't
  // currently make any difference. but, we'll maybe need it
  // sooner or later, so we'll leave it.
  auto key = pack_xattr_key(ino, name);
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &xattr_node_fetch);

  key = pack_xattr_data_key(ino, name);
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &xattr_data_fetch);

  return std::bind(&Inflight_getxattr::process, this);
}

extern "C" void fdbfs_getxattr(fuse_req_t req, fuse_ino_t ino,
			       const char *name, size_t size)
{
  if(filename_length_check(req, name))
    return;

  Inflight_getxattr *inflight =
    new Inflight_getxattr(req, ino, name, size, make_transaction());
  inflight->start();
}
