
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

#include "util.h"
#include "inflight.h"

/*************************************************************
 * forget
 *************************************************************
 */
class Inflight_forget : public Inflight {
public:
  Inflight_forget(fuse_req_t, fuse_ino_t, unique_transaction);
  Inflight_forget *reincarnate();
  InflightCallback issue();
private:
  fuse_ino_t ino;
  unique_future commit;
};

Inflight_forget::Inflight_forget(fuse_req_t req, fuse_ino_t ino,
				     unique_transaction transaction)
  : Inflight(req, false, std::move(transaction)), ino(ino)
{
}

Inflight_forget *Inflight_forget::reincarnate()
{
  Inflight_forget *x = new Inflight_forget(req, ino,
					   std::move(transaction));
  delete this;
  return x;
}

InflightCallback Inflight_forget::issue()
{
  auto key = pack_inode_use_key(ino);

  // TODO this has a problem if fuse tells us to forget this
  // inode, and then while this clear transaction is inflight,
  // fuse looks up the inode again. that lookup will attempt to
  // reinsert the use record. whether this clear or that insert
  // happens first can't be determined by anything we've yet done.
  fdb_transaction_clear(transaction.get(), key.data(), key.size());
  
  wait_on_future(fdb_transaction_commit(transaction.get()),
		 &commit);
  return []() -> InflightAction {
    return InflightAction::None();
  };
}

extern "C" void fdbfs_forget(fuse_req_t req, fuse_ino_t ino, uint64_t ncount)
{
  // we've only got to issue an fdb transaction if decrement says so
  if(decrement_lookup_count(ino, ncount)) {
    Inflight_forget *inflight =
      new Inflight_forget(req, ino, make_transaction());
    inflight->start();
  } else {
    fuse_reply_none(req);
  }
}
