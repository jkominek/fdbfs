#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>

#include <nlohmann/json.hpp>

#include "util.h"
#include "inflight.h"

/*************************************************************
 * statfs
 *************************************************************
 * INITIAL PLAN
 * fetch \xFF\xFF/status/json and make some stuff up based on that
 *
 * REAL PLAN?
 * ???
 */

class Inflight_statfs : public Inflight {
public:
  Inflight_statfs(fuse_req_t, unique_transaction);
  InflightCallback issue();
  Inflight_statfs *reincarnate();
private:
  unique_future status_fetch;
  InflightAction process_status();
};

Inflight_statfs::Inflight_statfs(fuse_req_t req,
				 unique_transaction transaction)
  : Inflight(req, false, std::move(transaction))
{
}

Inflight_statfs *Inflight_statfs::reincarnate()
{
  Inflight_statfs *x = new Inflight_statfs(req, std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_statfs::process_status()
{
  return InflightAction::Abort(ENOSYS);
}

InflightCallback Inflight_statfs::issue()
{
  FDBFuture *f = fdb_transaction_get(transaction.get(),
				     reinterpret_cast<const uint8_t*>("\xff\xff/status/json"), 14, 1);
  wait_on_future(f, &status_fetch);
  return std::bind(&Inflight_statfs::process_status, this);
}

extern "C" void fdbfs_statfs(fuse_req_t req, fuse_ino_t ino)
{
  Inflight_statfs *inflight = new Inflight_statfs(req, make_transaction());
  inflight->start();
}
