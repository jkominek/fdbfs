#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>

#include "util.h"
#include "inflight.h"
#include "values.pb.h"

/*************************************************************
 * lookup
 *************************************************************
 * INITIAL PLAN
 * not too bad. given a directory inode and a name, we:
 *   1. fetch the directory entry, getting us an inode.
 *   2. getattr-equiv on that inode.
 *
 * REAL PLAN
 * might be a good spot for an optimization? we can finish off
 * the fuse request, but then maintain some inode cache with
 * the last unchanging attributes of an inode that we've seen.
 * we could reject invalid requests to inodes faster. (readdir
 * on a file, for instance?) is it worth it to make the error
 * case faster?
 *
 * TRANSACTIONAL BEHAVIOR
 * We're doing the two reads as snapshots. Since the filesystem
 * can change arbitrarily immediately after we're done, it doesn't
 * much matter if it changes by a little or a lot. Just want to
 * ensure that we show the user something that was true.
 */
class Inflight_lookup : public Inflight {
public:
  Inflight_lookup(fuse_req_t, fuse_ino_t, std::string, unique_transaction);
  InflightCallback issue();
  Inflight_lookup *reincarnate();
private:
  fuse_ino_t parent;
  std::string name;

  fuse_ino_t target;

  unique_future dirent_fetch;
  unique_future inode_fetch;

  // issue looks up the dirent and then...
  InflightAction lookup_inode();
  InflightAction process_inode();
};

Inflight_lookup::Inflight_lookup(fuse_req_t req,
				 fuse_ino_t parent,
				 std::string name,
				 unique_transaction transaction)
  : Inflight(req, false, std::move(transaction)), parent(parent), name(name)
{
}

Inflight_lookup *Inflight_lookup::reincarnate()
{
  Inflight_lookup *x = new Inflight_lookup(req, parent, name, std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_lookup::process_inode()
{
  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  if(fdb_future_get_value(inode_fetch.get(), &present,
			  (const uint8_t **)&val, &vallen)) {
    return InflightAction::Restart();
  }

  // and second callback, to get the attributes
  if(!present) {
    return InflightAction::Abort(EIO);
  }

  INodeRecord inode;
  inode.ParseFromArray(val, vallen);
  if(!inode.IsInitialized()) {
    return InflightAction::Abort(EIO);
  }

  auto e = std::make_unique<struct fuse_entry_param>();
  e->ino = target;
  // TODO technically we need to be smarter about generations
  e->generation = 1;
  pack_inode_record_into_stat(&inode, &(e->attr));
  e->attr_timeout = 0.01;
  e->entry_timeout = 0.01;

  return InflightAction::Entry(std::move(e));
}

InflightAction Inflight_lookup::lookup_inode()
{
  fdb_bool_t present=0;
  uint8_t *val;
  int vallen;
  if(fdb_future_get_value(dirent_fetch.get(),
			  &present, (const uint8_t **)&val, &vallen)) {
    return InflightAction::Restart();
  }

  // we're on the first callback, to get the directory entry
  if(present) {
    {
      DirectoryEntry dirent;
      dirent.ParseFromArray(val, vallen);
      if(!dirent.has_inode()) {
	throw std::runtime_error("directory entry missing inode");
      }
      target = dirent.inode();
    }

    auto key = pack_inode_key(target);

    // and request just that inode
    FDBFuture *f = fdb_transaction_get(transaction.get(),
				       key.data(), key.size(), 1);
    wait_on_future(f, &inode_fetch);
    return InflightAction::BeginWait(std::bind(&Inflight_lookup::process_inode, this));
  } else {
    return InflightAction::Abort(ENOENT);
  }
}

InflightCallback Inflight_lookup::issue()
{
  auto key = pack_dentry_key(parent, name);

  FDBFuture *f = fdb_transaction_get(transaction.get(),
				     key.data(), key.size(), 1);

  wait_on_future(f, &dirent_fetch);
  return std::bind(&Inflight_lookup::lookup_inode, this);
}

extern "C" void fdbfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  std::string sname(name);
  Inflight_lookup *inflight = new Inflight_lookup(req, parent, sname, make_transaction());
  inflight->start();
}
