#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <nlohmann/json.hpp>

#include "fdbfs_ops.h"
#include "inflight.h"
#include "util.h"

/*************************************************************
 * statfs
 *************************************************************
 * INITIAL PLAN
 * fetch \xFF\xFF/status/json and make some stuff up based on that
 *
 * REAL PLAN?
 * ???
 */

struct AttemptState_statfs : public AttemptState {
  unique_future status_fetch;
};

class Inflight_statfs : public InflightWithAttempt<AttemptState_statfs> {
public:
  Inflight_statfs(fuse_req_t, unique_transaction);
  InflightCallback issue();

private:
  InflightAction process_status();
};

Inflight_statfs::Inflight_statfs(fuse_req_t req, unique_transaction transaction)
    : InflightWithAttempt(req, ReadWrite::ReadOnly, std::move(transaction)) {}

InflightAction Inflight_statfs::process_status() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err =
      fdb_future_get_value(a().status_fetch.get(), &present, &val, &vallen);
  if (err)
    return InflightAction::FDBError(err);
  if (!present)
    return InflightAction::Abort(ENOSYS);

  const std::string buffer(reinterpret_cast<const char *>(val), vallen);

  fsblkcnt_t used_blocks = 0;
  fsblkcnt_t min_available_blocks = 1000; // UINT64_MAX;

  // be careful when using the JSON stuff to avoid generating any exceptions
  // or unhandled errors. we've got to walk through a bunch of JSON and that
  // can go wrong at almost every step. 🙄
  nlohmann::json status = nlohmann::json::parse(buffer, nullptr, false);
  if (status.is_discarded())
    return InflightAction::Abort(EIO);

  // very rough estimation of space used and space available in the
  // fdb cluster

  auto cluster_it = status.find("cluster");
  if ((cluster_it == status.end()) || !cluster_it->is_object())
    return InflightAction::Abort(EIO);
  auto processes_it = cluster_it->find("processes");
  if ((processes_it == cluster_it->end()) || !processes_it->is_object())
    return InflightAction::Abort(EIO);
  auto &processes = *processes_it;

  for (auto it = processes.begin(); it != processes.end(); ++it) {
    auto roles_it = it.value().find("roles");
    if ((roles_it == it.value().end()) || !roles_it->is_array())
      continue;
    auto &roles = *roles_it;
    for (auto jt = roles.begin(); jt != roles.end(); ++jt) {
      if (!jt->is_object())
        continue;
      auto role_it = jt->find("role");
      if ((role_it == jt->end()) || !role_it->is_string() ||
          role_it->get_ref<const std::string &>() != "storage")
        continue;
      auto used_it = jt->find("kvstore_used_bytes");
      auto available_it = jt->find("kvstore_available_bytes");
      if ((used_it == jt->end()) || (available_it == jt->end()) ||
          !used_it->is_number_unsigned() || !available_it->is_number_unsigned())
        continue;
      const fsblkcnt_t used_blocks_for_process =
          used_it->template get<uint64_t>() / BLOCKSIZE;
      const fsblkcnt_t available_blocks_for_process =
          available_it->template get<uint64_t>() / BLOCKSIZE;
      used_blocks += used_blocks_for_process;
      min_available_blocks =
          std::min(min_available_blocks, available_blocks_for_process);
    }
  }

  auto output = std::make_unique<struct statvfs>();
  output->f_bsize = BLOCKSIZE;
  output->f_frsize = BLOCKSIZE;
  output->f_blocks = used_blocks + min_available_blocks;
  output->f_bfree = min_available_blocks;
  output->f_bavail = min_available_blocks;

  output->f_files = 1;
  output->f_ffree = 1 << 24; // "lots"
  output->f_favail = 1 << 24;

  output->f_fsid = 0;
  output->f_flag = 0;
  output->f_namemax = 1024;

  return InflightAction::Statfs(std::move(output));
}

InflightCallback Inflight_statfs::issue() {
  wait_on_future(fdb_transaction_get(
                     transaction.get(),
                     reinterpret_cast<const uint8_t *>("\xff\xff/status/json"),
                     14, 1),
                 a().status_fetch);
  return std::bind(&Inflight_statfs::process_status, this);
}

extern "C" void fdbfs_statfs(fuse_req_t req, fuse_ino_t ino) {
  Inflight_statfs *inflight = new Inflight_statfs(req, make_transaction());
  inflight->start();
}
