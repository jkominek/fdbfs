
#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 600
#include <foundationdb/fdb_c.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>

#include "util.h"
#include "inflight.h"

/******************************************************
 * This encapsulates all of our retry logic
 *
 * Every sub-struct of fdbfs_inflight_base will represent
 * an async fuse request that we're in the process of handling.
 *
 * cb will point to the function which will handle the final
 * processing of the result, and send a result back to fuse.
 * if there's only one future being used, it will be called
 * only once. with multiple futures, it might send later futures
 * back to the error_checker.
 *
 * issuer takes a filled out struct and issues the FDB calls
 * to start things. the relevant future is set to fdbfs_error_checker.
 * it may be called over and over by the error_processor.
 *
 * fdbfs_error_checker checks the future for an error. if things are
 * ok, it passes along to cb. if there has been a problem, it uses
 * fdb_transaction_on_error and sets its callback to fdbfs_error_processor.
 *
 * fdbfs_error_processor will abort the fuse request if there was a
 * failure. otherwise it will use issuer to start the transaction over
 * again.
 */

// allocate an inflight struct and fill and out some basics.
// readwrite specifies the transaction will include writes
void *fdbfs_inflight_create(size_t s, fuse_req_t req, FDBCallback cb, void (*issuer)(void *p), fdb_bool_t readwrite)
{
  struct fdbfs_inflight_base *inflight = malloc(s);

  // we need to be more clever about this. having every single
  // operation fetch a read version is going to add a lot of latency.
  fdb_database_create_transaction(database, &(inflight->transaction));
  inflight->req = req;
  inflight->cb = cb;
  inflight->issuer = issuer;

  return inflight;
}

// deallocate an inflight
void fdbfs_inflight_cleanup(struct fdbfs_inflight_base *inflight)
{
  fdb_transaction_destroy(inflight->transaction);
  free(inflight);
}

void fdbfs_error_processor(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_base *inflight = (struct fdbfs_inflight_base *)p;
  fdb_error_t err = fdb_future_get_error(f);
  // done with this either way.
  fdb_future_destroy(f);

  if(err) {
    debug_print("fdbfs_error_processor killing request %p for inflight %p: %s",
		inflight->req, p, fdb_get_error(err));
    // error during an error. foundationdb says that means
    // you should give up. so we'll let fuse know they're hosed.
    fuse_reply_err(inflight->req, EIO);

    fdbfs_inflight_cleanup(inflight);
    return;
  }

  // foundationdb, perhaps after some delay, has given us the
  // goahead to attempt our transaction again.
  inflight->issuer(p);
}

void fdbfs_error_checker(FDBFuture *f, void *p)
{
  struct fdbfs_inflight_base *inflight = (struct fdbfs_inflight_base *)p;
  fdb_error_t err = fdb_future_get_error(f);

  if(err) {
    // got an error during normal processing. foundationdb says
    // we should call _on_error on it, and maybe we'll get to
    // try again, and maybe we won't.
    FDBFuture *nextf = fdb_transaction_on_error(inflight->transaction, err);
    fdb_future_destroy(f);
    fdb_future_set_callback(nextf, fdbfs_error_processor, p);
    return;
  }

  inflight->cb(f, p);  
}

