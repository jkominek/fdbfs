
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
 * getxattr
 *************************************************************
 * INITIAL PLAN
 * two stage get
 *
 * REAL PLAN
 * ???
 */
template <typename ActionT>
struct AttemptState_getxattr : public AttemptStateT<ActionT> {
  unique_future xattr_node_fetch;
  unique_future xattr_data_fetch;
};

template <typename ActionT>
class Inflight_getxattr
    : public InflightWithAttemptT<AttemptState_getxattr<ActionT>,
                                  InflightPolicyReadOnly, ActionT> {
public:
  using Base = InflightWithAttemptT<AttemptState_getxattr<ActionT>,
                                    InflightPolicyReadOnly, ActionT>;
  using Base::a;
  using Base::transaction;
  using Base::wait_on_future;

  Inflight_getxattr(fuse_req_t, fuse_ino_t, std::string, size_t,
                    unique_transaction);
  InflightCallbackT<ActionT> issue();

private:
  const fuse_ino_t ino;
  const std::string name;
  const size_t maxsize;

  ActionT process();
};

template <typename ActionT>
Inflight_getxattr<ActionT>::Inflight_getxattr(
    fuse_req_t req, fuse_ino_t ino, std::string name, size_t maxsize,
    unique_transaction transaction)
    : Base(req, std::move(transaction)),
      ino(ino), name(name), maxsize(maxsize) {}

template <typename ActionT>
ActionT Inflight_getxattr<ActionT>::process() {
  fdb_bool_t present = 0;
  const uint8_t *val = nullptr;
  int vallen;
  fdb_error_t err;

  err =
      fdb_future_get_value(a().xattr_node_fetch.get(), &present, &val, &vallen);
  if (err)
    return ActionT::FDBError(err);

  if (present) {
    // decode xattr node metadata to drive payload decoding
    XAttrRecord xattr;
    if (!xattr.ParseFromArray(val, vallen)) {
      return ActionT::Abort(EIO);
    }
    if (!xattr.IsInitialized() || !xattr.has_size()) {
      return ActionT::Abort(EIO);
    }

    const XAttrEncoding encoding = xattr.encoding();

    err = fdb_future_get_value(a().xattr_data_fetch.get(), &present, &val,
                               &vallen);
    if (err)
      return ActionT::FDBError(err);

    const size_t true_size = static_cast<size_t>(xattr.size());

    if (maxsize == 0) {
      return ActionT::XattrSize(true_size);
    }

    if (true_size > maxsize) {
      return ActionT::Abort(ERANGE);
    }

    std::span<const uint8_t> stored_payload;
    if (present) {
      stored_payload =
          std::span<const uint8_t>(val, static_cast<size_t>(vallen));
    } else {
      stored_payload = std::span<const uint8_t>();
    }

    std::vector<uint8_t> buffer(true_size, 0);
    auto decode_ret = decode_logical_payload_slice(
        encoding, stored_payload, true_size, 0, std::span<uint8_t>(buffer));
    if (!decode_ret) {
      return ActionT::Abort(decode_ret.error());
    }
    if (decode_ret.value() != true_size) {
      return ActionT::Abort(EIO);
    }
    return ActionT::Buf(buffer);
  } else {
    return ActionT::Abort(ENODATA);
  }
}

template <typename ActionT>
InflightCallbackT<ActionT> Inflight_getxattr<ActionT>::issue() {
  // we could just not fetch the node, since it doesn't
  // currently make any difference. but, we'll maybe need it
  // sooner or later, so we'll leave it.
  {
    const auto key = pack_xattr_key(ino, name);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        a().xattr_node_fetch);
  }

  {
    const auto key = pack_xattr_data_key(ino, name);
    wait_on_future(
        fdb_transaction_get(transaction.get(), key.data(), key.size(), 0),
        a().xattr_data_fetch);
  }

  return std::bind(&Inflight_getxattr<ActionT>::process, this);
}

extern "C" void fdbfs_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                               size_t size) {
  if (filename_length_check(name)) {
    fuse_reply_err(req, ENAMETOOLONG);
    return;
  }

  auto *inflight = new Inflight_getxattr<FuseInflightAction>(
      req, ino, name, size, make_transaction());
  inflight->start();
}
