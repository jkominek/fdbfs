
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 730
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

/*************************************************************
 * removexattr
 *************************************************************
 * INITIAL PLAN
 * fetch xattr to get the xnode, then clear them
 *
 * REAL PLAN
 * ???
 */
struct AttemptState_removexattr : public AttemptState {
  unique_future xattr_node_fetch;
};

class Inflight_removexattr
    : public InflightWithAttempt<AttemptState_removexattr> {
public:
  Inflight_removexattr(fuse_req_t, fuse_ino_t, std::string, unique_transaction);
  InflightCallback issue();

private:
  const fuse_ino_t ino;
  const std::string name;

  fdb_error_t configure_transaction() override;
  InflightAction process();
  InflightAction oplog_recovery(const OpLogRecord &) override;
  bool write_success_oplog_result();
};

Inflight_removexattr::Inflight_removexattr(fuse_req_t req, fuse_ino_t ino,
                                           std::string name,
                                           unique_transaction transaction)
    : InflightWithAttempt(req, ReadWrite::Yes, std::move(transaction)),
      ino(ino), name(std::move(name)) {}

fdb_error_t Inflight_removexattr::configure_transaction() {
  return fdb_transaction_set_option(transaction.get(),
                                    FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE,
                                    nullptr, 0);
}

bool Inflight_removexattr::write_success_oplog_result() {
  OpLogResultOK result;
  return write_oplog_result(result);
}

InflightAction Inflight_removexattr::oplog_recovery(const OpLogRecord &record) {
  if (record.result_case() != OpLogRecord::kOk) {
    return InflightAction::Abort(EIO);
  }
  return InflightAction::OK();
}

InflightAction Inflight_removexattr::process() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(a().xattr_node_fetch.get(), &present, &val,
                             &vallen);
  if (err)
    return InflightAction::FDBError(err);

  if (present) {
    if (!write_success_oplog_result()) {
      return InflightAction::Abort(EIO);
    }
    return commit(InflightAction::OK);
  } else {
    return InflightAction::Abort(ENODATA);
  }
}

InflightCallback Inflight_removexattr::issue() {
  {
    const auto key = pack_xattr_key(ino, name);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        a().xattr_node_fetch);

    fdb_transaction_clear(transaction.get(), key.data(), key.size());
  }

  {
    const auto key = pack_xattr_data_key(ino, name);
    fdb_transaction_clear(transaction.get(), key.data(), key.size());
  }

  return std::bind(&Inflight_removexattr::process, this);
}

extern "C" void fdbfs_removexattr(fuse_req_t req, fuse_ino_t ino,
                                  const char *name) {
  if (filename_length_check(req, name))
    return;

  std::string sname(name);
  Inflight_removexattr *inflight =
      new Inflight_removexattr(req, ino, sname, make_transaction());
  inflight->start();
}
