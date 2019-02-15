#ifndef __INFLIGHT_H_
#define __INFLIGHT_H_

struct fdbfs_inflight_base {
  FDBTransaction *transaction;
  FDBCallback cb;
  void (*issuer)(void *p);
  fuse_req_t req;
};

extern void *fdbfs_inflight_create(size_t s, fuse_req_t req, FDBCallback cb, void (*issuer)(void *p));
extern void fdbfs_inflight_cleanup(struct fdbfs_inflight_base *inflight);
extern void fdbfs_error_processor(FDBFuture *, void *);
extern void fdbfs_error_checker(FDBFuture *f, void *p);

#endif // __INFLIGHT_H_
