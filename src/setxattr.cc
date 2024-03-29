
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
#include <sys/xattr.h>

#include "fdbfs_ops.h"
#include "inflight.h"
#include "util.h"

enum SetXattrBehavior {
  CanCreate = 0x1,
  CanReplace = 0x2,
  CreateOrReplace = 0x3
};

/*************************************************************
 * setxattr
 *************************************************************
 * INITIAL PLAN
 * get the xattr node, if present. make determination about error
 * status. then set the xattr data.
 *
 * REAL PLAN
 * ???
 */
class Inflight_setxattr : public Inflight {
public:
  Inflight_setxattr(fuse_req_t, fuse_ino_t, std::string, std::vector<uint8_t>,
                    SetXattrBehavior, unique_transaction);
  Inflight_setxattr *reincarnate();
  InflightCallback issue();

private:
  fuse_ino_t ino;
  std::string name;
  std::vector<uint8_t> xattr_value;
  SetXattrBehavior behavior;

  unique_future xattr_node_fetch;

  InflightAction process();
};

Inflight_setxattr::Inflight_setxattr(fuse_req_t req, fuse_ino_t ino,
                                     std::string name,
                                     std::vector<uint8_t> xattr_value,
                                     SetXattrBehavior behavior,
                                     unique_transaction transaction)
    : Inflight(req, ReadWrite::Yes, std::move(transaction)), ino(ino),
      name(name), xattr_value(xattr_value), behavior(behavior) {}

Inflight_setxattr *Inflight_setxattr::reincarnate() {
  Inflight_setxattr *x = new Inflight_setxattr(
      req, ino, name, xattr_value, behavior, std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_setxattr::process() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(xattr_node_fetch.get(), &present, &val, &vallen);
  if (err)
    return InflightAction::FDBError(err);

  XAttrRecord xattr;
  if (present) {
    if (behavior & CanReplace) {
      xattr.ParseFromArray(val, vallen);
    } else {
      return InflightAction::Abort(EEXIST);
    }
  } else { // xattr not present
    if (behavior & CanCreate) {
      // set the xattr node

      const auto node_key = pack_xattr_key(ino, name);
      const int xattr_size = xattr.ByteSizeLong();
      uint8_t xattr_buffer[xattr_size];
      xattr.SerializeToArray(xattr_buffer, xattr_size);

      fdb_transaction_set(transaction.get(), node_key.data(), node_key.size(),
                          xattr_buffer, xattr_size);
    } else {
      return InflightAction::Abort(ENODATA);
    }
  }

  // set the xattr data
  const auto data_key = pack_xattr_data_key(ino, name);
  fdb_transaction_set(transaction.get(), data_key.data(), data_key.size(),
                      xattr_value.data(), xattr_value.size());

  return commit(InflightAction::OK);
}

InflightCallback Inflight_setxattr::issue() {
  const auto key = pack_xattr_key(ino, name);

  // and request just that xattr node
  wait_on_future(
      fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
      xattr_node_fetch);

  return std::bind(&Inflight_setxattr::process, this);
}

extern "C" void fdbfs_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                               const char *value, size_t size, int flags) {
  if (filename_length_check(req, name))
    return;

  SetXattrBehavior behavior = CreateOrReplace;
  if (flags == XATTR_CREATE)
    behavior = CanCreate;
  else if (flags == XATTR_REPLACE)
    behavior = CanReplace;

  std::string sname(name);
  std::vector<uint8_t> vvalue(value, value + size);

  Inflight_setxattr *inflight = new Inflight_setxattr(
      req, ino, sname, vvalue, behavior, make_transaction());
  inflight->start();
}
