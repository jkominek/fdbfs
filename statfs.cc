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
#include "fdbfs_ops.h"

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
  fdb_bool_t present = 0;
  uint8_t *val;
  int vallen;
  fdb_error_t err = fdb_future_get_value(status_fetch.get(),
					 &present,
					 (const uint8_t **)&val, &vallen);
  if(err)
    return InflightAction::FDBError(err);
  if(!present)
    return InflightAction::Abort(EIO);

  // nlohmann::json doesn't specify that it retquires
  // null-terminated arrays, but it does.
  uint8_t buffer[vallen+1];
  bcopy(val, buffer, vallen);
  buffer[vallen] = '\0';

  fsblkcnt_t used_blocks = 0;
  fsblkcnt_t min_available_blocks = 1000; //UINT64_MAX;

  nlohmann::json status = nlohmann::json::parse(buffer);

  // very rough estimation of space used and space available in the
  // fdb cluster

  auto processes = status["cluster"]["processes"];
  for(auto it = processes.begin(); it != processes.end(); ++it) {
    auto roles = it.value()["roles"];
    for(auto jt = roles.begin(); jt != roles.end(); ++jt) {
      if((*jt)["role"] == "storage") {
        auto storage = *jt;
	//std::cout << storage << std::endl;
        used_blocks += ((unsigned long)storage["kvstore_used_bytes"])/BLOCKSIZE;
	//std::cout << ((unsigned long)storage["kvstore_used_bytes"]) << std::endl;
	//std::cout << ((unsigned long)storage["kvstore_available_bytes"]) << std::endl;
        min_available_blocks = std::min(min_available_blocks, ((unsigned long)storage["kvstore_available_bytes"])/BLOCKSIZE);
      }
    }
  }

  auto output = std::make_unique<struct statvfs>();
  output->f_bsize = BLOCKSIZE;
  output->f_frsize = BLOCKSIZE;
  output->f_blocks = used_blocks + min_available_blocks;
  output->f_bfree = min_available_blocks;
  output->f_bavail = min_available_blocks;

  output->f_files = 1;
  output->f_ffree = 1<<24; // "lots"
  output->f_favail = 1<<24;

  output->f_fsid = 0;
  output->f_flag = 0;
  output->f_namemax = 1024;

  return InflightAction::Statfs(std::move(output));
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
