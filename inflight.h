#ifndef __INFLIGHT_H_
#define __INFLIGHT_H_

struct fdbfs_inflight_base {
  // the transaction we're tied to. will we ever want
  // two separate chains of computation using the same
  // transaction?
  FDBTransaction *transaction;
  // cb is what we _want_ to run if whatever future we're
  // waiting on succeeds. if we need to take multiple steps,
  // they can change this as they progress. issuer, below,
  // should assume it needs to set cb appropriately.
  FDBCallback cb;
  // issuer is what we'll have to run if the future fails.
  // it'll expect the transaction to have been reset
  // (which is guaranteed by fdb)
  void (*issuer)(void *p);
  // always need this so that our error processing code
  // can throw errors back to fuse.
  fuse_req_t req;
};

extern void *fdbfs_inflight_create(size_t s, fuse_req_t req, FDBCallback cb, void (*issuer)(void *p));
extern void fdbfs_inflight_cleanup(struct fdbfs_inflight_base *inflight);
extern void fdbfs_error_processor(FDBFuture *, void *);
extern void fdbfs_error_checker(FDBFuture *f, void *p);

#endif // __INFLIGHT_H_
