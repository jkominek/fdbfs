
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <attr/xattr.h>

#include "util.h"
#include "inflight.h"
#include "fdbfs_ops.h"

/*************************************************************
 * listxattr
 *************************************************************
 * INITIAL PLAN
 * this is complicated because it has a mechanism for returning
 * the listing, and another mechanism for figuring out how big
 * the listing will be.
 *
 * What's with all the "key_length - empty_xattr_name_length" stuff?
 * The keys are of the form concat(PREFIX, xattr_name).
 * key_length = len(concat(PREFIX, xattr_name)) and
 * empty_xattr_name_length = len(concat(PREFIX, ""))
 * So subtracting that off gets the length of xattr_name, which is
 * what we're interested in.
 *
 * Why do we compute empty_xattr_name_length at the start of every
 * transaction? It depends on the prefix/subspace for the filesystem
 * we're operating on. It could be determined just once at run time,
 * but we haven't implemented a place for all of the do-once-at-run-time
 * precomputations.
 *
 * REAL PLAN
 * ???
 */
class Inflight_listxattr : public Inflight {
public:
  Inflight_listxattr(fuse_req_t, fuse_ino_t, size_t, unique_transaction);
  Inflight_listxattr *reincarnate();
  InflightCallback issue();
private:
  fuse_ino_t ino;
  size_t maxsize;
  int empty_xattr_name_length;

  std::vector<uint8_t> buf;

  unique_future range_fetch;
  InflightAction process();
};

Inflight_listxattr::Inflight_listxattr(fuse_req_t req, fuse_ino_t ino,
				       size_t maxsize,
				       unique_transaction transaction)
  : Inflight(req, ReadWrite::ReadOnly, std::move(transaction)),
    ino(ino), maxsize(maxsize), buf()
{
  buf.reserve(maxsize);
}

Inflight_listxattr *Inflight_listxattr::reincarnate()
{
  Inflight_listxattr *x = new Inflight_listxattr(req, ino, maxsize,
					       std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_listxattr::process()
{
  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_error_t err;
  
  err = fdb_future_get_keyvalue_array(range_fetch.get(), &kvs, &kvcount, &more);
  if(err) return InflightAction::FDBError(err);

  // in both of these, we'd ideally immediately check 'more' to see
  // if we need to request additional data, and do so before spending
  // time processing things. but that would risk overwriting the future
  // that we're still pulling things out of. so...
  // TODO make the code more clever, and do this the better way
  
  for(int i=0; i<kvcount; i++) {
    const FDBKeyValue *kv = kvs + i;

    int remaining_length = kv->key_length - empty_xattr_name_length;
    if(buf.size() + remaining_length + 1 > maxsize) {
      // they didn't provide us with a large enough buffer
      return InflightAction::Abort(ERANGE);
    }

    auto k = kv->key + empty_xattr_name_length;
    buf.insert(buf.end(), k, k + remaining_length);
    buf.push_back(0);
  }

  if((maxsize <= buf.size()) || (!more)) {
    // return the buffer
    return InflightAction::Buf(buf);
  }

  const FDBKeyValue *lastkv = kvs + (kvcount - 1);
  const auto stop = pack_xattr_key(ino, "\xFF");

  // apparently, there is more, and we've got space
  wait_on_future(fdb_transaction_get_range(transaction.get(),
					   lastkv->key,
					   lastkv->key_length, 0, 1,
					   stop.data(), stop.size(), 0, 1,
					   maxsize - buf.size(), 0,
					   FDB_STREAMING_MODE_WANT_ALL, 0,
					   0, 0),
		 range_fetch);
  return InflightAction::BeginWait(std::bind(&Inflight_listxattr::process, this));
}

InflightCallback Inflight_listxattr::issue()
{
  const auto start = pack_xattr_key(ino, "");
  const auto stop = pack_xattr_key(ino, "\xFF");

  wait_on_future(fdb_transaction_get_range(transaction.get(),
					   start.data(), start.size(), 0, 1,
					   stop.data(), stop.size(), 0, 1,
					   maxsize, 0,
					   FDB_STREAMING_MODE_WANT_ALL, 0,
					   0, 0),
		 range_fetch);

  empty_xattr_name_length = start.size();

  return std::bind(&Inflight_listxattr::process, this);
}

class Inflight_listxattr_count : public Inflight {
public:
  Inflight_listxattr_count(fuse_req_t, fuse_ino_t, unique_transaction);
  Inflight_listxattr_count *reincarnate();
  InflightCallback issue();
private:
  fuse_ino_t ino;
  int empty_xattr_name_length;

  ssize_t accumulated_size = 0;

  unique_future range_fetch;
  InflightAction process();
};

Inflight_listxattr_count::Inflight_listxattr_count(fuse_req_t req,
						   fuse_ino_t ino,
						   unique_transaction transaction)
  : Inflight(req, ReadWrite::ReadOnly, std::move(transaction)),
    ino(ino)
{
}

Inflight_listxattr_count *Inflight_listxattr_count::reincarnate()
{
  Inflight_listxattr_count *x =
    new Inflight_listxattr_count(req, ino, std::move(transaction));
  delete this;
  return x;
}

InflightAction Inflight_listxattr_count::process()
{
  const FDBKeyValue *kvs;
  int kvcount;
  fdb_bool_t more;
  fdb_error_t err;
  
  err = fdb_future_get_keyvalue_array(range_fetch.get(), &kvs, &kvcount, &more);
  if(err) return InflightAction::FDBError(err);

  for(int i=0; i<kvcount; i++) {
    const FDBKeyValue *kv = kvs + i;
    accumulated_size += (kv->key_length - empty_xattr_name_length + 1);
  }

  const FDBKeyValue *lastkv = kvs + (kvcount - 1);
  const auto stop = pack_xattr_key(ino, "\xFF");

  if(more) {
    wait_on_future(fdb_transaction_get_range(transaction.get(),
					     lastkv->key,
					     lastkv->key_length, 0, 1,
					     stop.data(), stop.size(), 0, 1,
					     0, 0,
					     FDB_STREAMING_MODE_WANT_ALL, 0,
					     0, 0),
		   range_fetch);
    return InflightAction::BeginWait(std::bind(&Inflight_listxattr_count::process, this));
  } else {
    return InflightAction::XattrSize(accumulated_size);
  }
}

InflightCallback Inflight_listxattr_count::issue()
{
  const auto start = pack_xattr_key(ino, "");
  const auto stop = pack_xattr_key(ino, "\xFF");

  wait_on_future(fdb_transaction_get_range(transaction.get(),
					   start.data(), start.size(), 0, 1,
					   stop.data(), stop.size(), 0, 1,
					   0, 0,
					   FDB_STREAMING_MODE_WANT_ALL, 0,
					   0, 0),
		 range_fetch);

  empty_xattr_name_length = start.size();

  return std::bind(&Inflight_listxattr_count::process, this);
}

extern "C" void fdbfs_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size)
{
  if(size == 0) {
    Inflight_listxattr_count *inflight =
      new Inflight_listxattr_count(req, ino, make_transaction());
    inflight->start();
  } else {
    Inflight_listxattr *inflight =
      new Inflight_listxattr(req, ino, size, make_transaction());
    inflight->start();
  }
}
