
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

#include <limits>

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
struct AttemptState_setxattr : public AttemptState {
  unique_future xattr_node_fetch;
};

class Inflight_setxattr : public InflightWithAttempt<AttemptState_setxattr> {
public:
  Inflight_setxattr(fuse_req_t, fuse_ino_t, std::string, std::vector<uint8_t>,
                    SetXattrBehavior, unique_transaction);
  InflightCallback issue();

private:
  const fuse_ino_t ino;
  const std::string name;
  const std::vector<uint8_t> xattr_value;
  const SetXattrBehavior behavior;

  InflightAction process();
  InflightAction oplog_recovery(const OpLogRecord &) override;
  bool write_success_oplog_result();
};

Inflight_setxattr::Inflight_setxattr(fuse_req_t req, fuse_ino_t ino,
                                     std::string name,
                                     std::vector<uint8_t> xattr_value,
                                     SetXattrBehavior behavior,
                                     unique_transaction transaction)
    : InflightWithAttempt(req, ReadWrite::Yes, std::move(transaction)),
      ino(ino), name(std::move(name)), xattr_value(std::move(xattr_value)),
      behavior(behavior) {}

bool Inflight_setxattr::write_success_oplog_result() {
  OpLogResultOK result;
  return write_oplog_result(result);
}

InflightAction Inflight_setxattr::oplog_recovery(const OpLogRecord &record) {
  if (record.result_case() != OpLogRecord::kOk) {
    return InflightAction::Abort(EIO);
  }
  return InflightAction::OK();
}

InflightAction Inflight_setxattr::process() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;

  err = fdb_future_get_value(a().xattr_node_fetch.get(), &present, &val,
                             &vallen);
  if (err)
    return InflightAction::FDBError(err);

  XAttrRecord xattr;
  if (present) {
    if (behavior & CanReplace) {
      if (!xattr.ParseFromArray(val, vallen))
        return InflightAction::Abort(EIO);
    } else {
      return InflightAction::Abort(EEXIST);
    }
  } else { // xattr not present
    if (!(behavior & CanCreate)) {
      return InflightAction::Abort(ENODATA);
    }
  }

  auto encoded = encode_logical_payload(std::span<const uint8_t>(xattr_value));
  if (!encoded)
    return InflightAction::Abort(encoded.error());
  if (encoded->true_block_size >
      static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
    return InflightAction::Abort(EOVERFLOW);
  }

  xattr.set_size(static_cast<uint32_t>(encoded->true_block_size));
  xattr.set_encoding(encoded->encoding);
  // update xattr node metadata
  if (!fdb_set_protobuf(transaction.get(), pack_xattr_key(ino, name), xattr))
    return InflightAction::Abort(EIO);

  // set/clear xattr data payload
  const auto data_key = pack_xattr_data_key(ino, name);
  if (encoded->bytes.empty()) {
    fdb_transaction_clear(transaction.get(), data_key.data(), data_key.size());
  } else {
    fdb_transaction_set(transaction.get(), data_key.data(), data_key.size(),
                        encoded->bytes.data(), encoded->bytes.size());
  }

  if (!write_success_oplog_result()) {
    return InflightAction::Abort(EIO);
  }

  return commit(InflightAction::OK);
}

InflightCallback Inflight_setxattr::issue() {
  const auto key = pack_xattr_key(ino, name);

  // and request just that xattr node
  wait_on_future(
      fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
      a().xattr_node_fetch);

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
