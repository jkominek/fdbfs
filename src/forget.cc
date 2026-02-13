
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "fdbfs_ops.h"
#include "inflight.h"
#include "util.h"

/*************************************************************
 * forget
 *************************************************************
 */
struct AttemptState_forget : public AttemptState {};

class Inflight_forget : public InflightWithAttempt<AttemptState_forget> {
public:
  Inflight_forget(fuse_req_t, std::vector<fuse_ino_t>, unique_transaction);
  InflightCallback issue();

private:
  const std::vector<fuse_ino_t> inos;
};

Inflight_forget::Inflight_forget(fuse_req_t req, std::vector<fuse_ino_t> inos,
                                 unique_transaction transaction)
    : InflightWithAttempt(req, ReadWrite::ReadOnly, std::move(transaction)),
      inos(std::move(inos)) {}

InflightCallback Inflight_forget::issue() {
  for (auto it = inos.cbegin(); it != inos.cend(); it++) {
    auto key = pack_inode_use_key(*it);

    // TODO this has a problem if fuse tells us to forget this
    // inode, and then while this clear transaction is inflight,
    // fuse looks up the inode again. that lookup will attempt to
    // reinsert the use record. whether this clear or that insert
    // happens first can't be determined by anything we've yet done.
    fdb_transaction_clear(transaction.get(), key.data(), key.size());
  }

  wait_on_future(fdb_transaction_commit(transaction.get()), a().commit);
  return InflightAction::None;
}

extern "C" void fdbfs_forget(fuse_req_t req, fuse_ino_t ino, uint64_t ncount) {
  // we've only got to issue an fdb transaction if decrement says so
  if (decrement_lookup_count(ino, ncount)) {
    std::vector<fuse_ino_t> inos(1);
    inos[0] = ino;
    Inflight_forget *inflight =
        new Inflight_forget(req, inos, make_transaction());
    inflight->start();
  } else {
    fuse_reply_none(req);
  }
}

extern "C" void fdbfs_forget_multi(fuse_req_t req, size_t count,
                                   struct fuse_forget_data *forgets) {
  std::vector<fuse_ino_t> inos;
  inos.reserve(count);
  for (size_t i = 0; i < count; i++) {
    if (decrement_lookup_count(forgets[i].ino, forgets[i].nlookup)) {
      inos.push_back(forgets[i].ino);
    }
  }
  if (inos.size() > 0) {
    // we've got to issue forgets
    Inflight_forget *inflight =
        new Inflight_forget(req, inos, make_transaction());
    inflight->start();
  } else {
    fuse_reply_none(req);
  }
}
