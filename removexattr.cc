
#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
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
 * removexattr
 *************************************************************
 * INITIAL PLAN
 * fetch xattr to get the xnode, then clear them
 *
 * REAL PLAN
 * ???
 */
class Inflight_removexattr : public Inflight {
public:
  Inflight_removexattr(fuse_req_t, fuse_ino_t, std::string,
		       unique_transaction);
  Inflight_removexattr *reincarnate();
  InflightCallback issue();
private:
  fuse_ino_t ino;
  std::string name;

  unique_future xattr_node_fetch;
  unique_future commit;
  InflightAction process();
  InflightAction commit_cb();
};

Inflight_removexattr::Inflight_removexattr(fuse_req_t req, fuse_ino_t ino,
					   std::string name,
					   unique_transaction transaction)
  : Inflight(req, false, std::move(transaction)),
    ino(ino), name(name)
{
}

Inflight_removexattr *Inflight_removexattr::reincarnate()
{
  Inflight_removexattr *x = new Inflight_removexattr(req, ino, name,
						     std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_removexattr::commit_cb()
{
  return InflightAction::OK();
}

InflightAction Inflight_removexattr::process()
{
  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(xattr_node_fetch.get(), &present, (const uint8_t **)&val, &vallen);
  if(err) return InflightAction::FDBError(err);

  if(present) {
    wait_on_future(fdb_transaction_commit(transaction.get()),
		   &commit);

    return InflightAction::BeginWait(std::bind(&Inflight_removexattr::commit_cb, this));
  } else {
    return InflightAction::Abort(ENOATTR);
  }
}

InflightCallback Inflight_removexattr::issue()
{
  // turn off RYW, so there's no uncertainty about what we'll get when
  // we interleave our reads and writes.
  if(fdb_transaction_set_option(transaction.get(),
                                FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE,
                                NULL, 0)) {
    // hmm.
    // TODO how do we generate an error here?
    return []() {
      // i don't think this will be run, since we've registered no futures?
      return InflightAction::Abort(EIO);
    };
  }

  auto key = pack_xattr_key(ino, name);

  wait_on_future(fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 0),
		 &xattr_node_fetch);

  fdb_transaction_clear(transaction.get(), key.data(), key.size());

  key = pack_xattr_data_key(ino, name);

  fdb_transaction_clear(transaction.get(), key.data(), key.size());

  return std::bind(&Inflight_removexattr::process, this);
}

extern "C" void fdbfs_removexattr(fuse_req_t req, fuse_ino_t ino,
				  const char *name)
{
  if(filename_length_check(req, name))
    return;

  std::string sname(name);
  Inflight_removexattr *inflight =
    new Inflight_removexattr(req, ino, sname, make_transaction());
  inflight->start();
}
