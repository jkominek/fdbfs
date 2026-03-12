
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/xattr.h>

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
template <typename ActionT>
struct AttemptState_removexattr : public AttemptStateT<ActionT> {
  unique_future xattr_node_fetch;
};

template <typename ActionT>
class Inflight_removexattr
    : public InflightWithAttemptT<AttemptState_removexattr<ActionT>,
                                  InflightPolicyWrite, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_removexattr<ActionT>,
                                    InflightPolicyWrite, ActionT>;
  using Base::a;
  using Base::commit;
  using Base::track_inode_for_fsync;
  using Base::transaction;
  using Base::wait_on_future;
  using Base::write_oplog_result;

  Inflight_removexattr(fuse_req_t, fuse_ino_t, std::string, unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  const fuse_ino_t ino;
  const std::string name;

  fdb_error_t configure_transaction() override;
  ActionT process();
  ActionT oplog_recovery(const OpLogRecord &) override;
  bool write_success_oplog_result();
};

template <typename ActionT>
Inflight_removexattr<ActionT>::Inflight_removexattr(
    fuse_req_t req, fuse_ino_t ino, std::string name,
    unique_transaction transaction)
    : Base(req, std::move(transaction)), ino(ino), name(std::move(name)) {
  track_inode_for_fsync(ino);
}

template <typename ActionT>
fdb_error_t Inflight_removexattr<ActionT>::configure_transaction() {
  return fdb_transaction_set_option(
      transaction.get(), FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, nullptr, 0);
}

template <typename ActionT>
bool Inflight_removexattr<ActionT>::write_success_oplog_result() {
  OpLogResultOK result;
  return write_oplog_result(result);
}

template <typename ActionT>
ActionT
Inflight_removexattr<ActionT>::oplog_recovery(const OpLogRecord &record) {
  if (record.result_case() != OpLogRecord::kOk) {
    return ActionT::Abort(EIO);
  }
  return ActionT::OK();
}

template <typename ActionT> ActionT Inflight_removexattr<ActionT>::process() {
  fdb_bool_t present = 0;
  const uint8_t *val;
  int vallen;
  fdb_error_t err;

  err =
      fdb_future_get_value(a().xattr_node_fetch.get(), &present, &val, &vallen);
  if (err)
    return ActionT::FDBError(err);

  if (present) {
    if (!write_success_oplog_result()) {
      return ActionT::Abort(EIO);
    }
    return commit(ActionT::OK);
  } else {
    return ActionT::Abort(ENODATA);
  }
}

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_removexattr<ActionT>::issue() {
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

  return std::bind(&Inflight_removexattr<ActionT>::process, this);
}
