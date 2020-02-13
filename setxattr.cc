
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

enum SetXattrBehavior
  {
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
  Inflight_setxattr(fuse_req_t, fuse_ino_t, std::string,
		    std::vector<uint8_t>, SetXattrBehavior,
		    unique_transaction);
  Inflight_setxattr *reincarnate();
  InflightCallback issue();
private:
  fuse_ino_t ino;
  std::string name;
  std::vector<uint8_t> xattr_value;
  SetXattrBehavior behavior;

  unique_future xattr_node_fetch;
  unique_future commit;
  InflightAction process();
  InflightAction commit_cb();
};

Inflight_setxattr::Inflight_setxattr(fuse_req_t req, fuse_ino_t ino,
				     std::string name,
				     std::vector<uint8_t> xattr_value,
				     SetXattrBehavior behavior,
				     unique_transaction transaction)
  : Inflight(req, false, std::move(transaction)),
    ino(ino), name(name), xattr_value(xattr_value), behavior(behavior)
{
}

Inflight_setxattr *Inflight_setxattr::reincarnate()
{
  Inflight_setxattr *x = new Inflight_setxattr(req, ino, name,
					       xattr_value, behavior,
					       std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_setxattr::commit_cb()
{
  return InflightAction::OK();
}

InflightAction Inflight_setxattr::process()
{
  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(xattr_node_fetch.get(), &present, (const uint8_t **)&val, &vallen);
  if(err) return InflightAction::FDBError(err);

  XAttrRecord xattr;
  if(present) {
    if(behavior & CanReplace) {
      xattr.ParseFromArray(val, vallen);
    } else {
      return InflightAction::Abort(EEXIST);
    }
  } else { // xattr not present
    if(behavior & CanCreate) {
      // set the xattr node
      xattr.set_xnode(generate_inode());
      auto node_key = pack_xattr_key(ino, name);
      int xattr_size = xattr.ByteSize();
      uint8_t xattr_buffer[xattr_size];
      xattr.SerializeToArray(xattr_buffer, xattr_size);
 
      fdb_transaction_set(transaction.get(),
			  node_key.data(), node_key.size(),
			  xattr_buffer, xattr_size);
    } else {
      return InflightAction::Abort(ENOATTR);
    }
  }

  // set the xattr data
  auto data_key = pack_xattr_data_key(ino, xattr.xnode());
  fdb_transaction_set(transaction.get(),
		      data_key.data(), data_key.size(),
		      xattr_value.data(), xattr_value.size());

  // commit
  wait_on_future(fdb_transaction_commit(transaction.get()),
		 &commit);

  return InflightAction::BeginWait(std::bind(&Inflight_setxattr::commit_cb, this));
}

InflightCallback Inflight_setxattr::issue()
{
  auto key = pack_xattr_key(ino, name);

  // if CanCreate, then maybe preemptively guess an inode number and see if it
  // exists?

  // and request just that xattr node
  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &xattr_node_fetch);

  return std::bind(&Inflight_setxattr::process, this);
}

extern "C" void fdbfs_setxattr(fuse_req_t req, fuse_ino_t ino,
			       const char *name, const char *value,
			       size_t size, int flags)
{
  SetXattrBehavior behavior = CreateOrReplace;
  if(flags == XATTR_CREATE)
    behavior = CanCreate;
  else if(flags == XATTR_REPLACE)
    behavior = CanReplace;

  std::string sname(name);
  std::vector<uint8_t> vvalue(value, value+size);
  
  Inflight_setxattr *inflight =
    new Inflight_setxattr(req, ino, sname, vvalue,
			  behavior,
			  make_transaction());
  inflight->start();
}
