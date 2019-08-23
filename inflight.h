#ifndef __INFLIGHT_H_
#define __INFLIGHT_H_

#define FDB_API_VERSION 610
#include <foundationdb/fdb_c.h>

#include <memory>
#include <deque>
#include <queue>
#include <functional>
#include <experimental/optional>

#ifdef DEBUG
#include <time.h>
#endif

struct FDBTransactionDeleter {
  void operator()(FDBTransaction *t) {
    fdb_transaction_destroy(t);
  }
};
struct FDBFutureDeleter {
  void operator()(FDBFuture *f) {
    fdb_future_destroy(f);
  }
};
typedef std::unique_ptr<FDBTransaction, FDBTransactionDeleter> unique_transaction;
typedef std::unique_ptr<FDBFuture, FDBFutureDeleter> unique_future;

class Inflight {
 public:
  // issuer is what we'll have to run if the future fails.
  // it'll expect the transaction to have been reset
  // (which is guaranteed by fdb)
  virtual void issue() = 0;

  // makes a new fresh version of the inflight object
  // and suicides the old one.
  virtual Inflight *reincarnate() = 0;
  void future_ready(FDBFuture *);

  virtual ~Inflight() = 0;
  
  // the transaction we're tied to. will we ever want
  // two separate chains of computation using the same
  // transaction?
  unique_transaction transaction;

  // always need this so that our error processing code
  // can throw errors back to fuse.
  fuse_req_t req;

 protected:
  // constructor
  Inflight(fuse_req_t, bool, FDBTransaction *transaction = NULL);
  
  void wait_on_future(FDBFuture *, unique_future *);
  void begin_wait();
  void restart();
  
  // inflight objects should call one of these at the end
  // to notify fuse of the outcome, and clean themselves up.
  // these all include 'delete this'
  void abort(int err);
  void reply_entry(struct fuse_entry_param *);
  void reply_attr(struct stat *);

  std::experimental::optional<std::function<void()>> cb;

 private:
  // whether we're intended as r/w or not.
  bool readwrite;
  std::queue<FDBFuture *> future_queue;

#ifdef DEBUG
  struct timespec start;
#endif
};

extern "C" void fdbfs_error_processor(FDBFuture *, void *);
extern "C" void fdbfs_error_checker(FDBFuture *, void *);

#endif // __INFLIGHT_H_
