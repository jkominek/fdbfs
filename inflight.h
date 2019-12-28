#ifndef __INFLIGHT_H_
#define __INFLIGHT_H_

#define FDB_API_VERSION 610
#include <foundationdb/fdb_c.h>

#include <memory>
#include <deque>
#include <queue>
#include <functional>
#include <optional>

#if DEBUG
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

unique_transaction make_transaction();

class InflightAction;

typedef std::function<InflightAction()> InflightCallback;

class Inflight {
 public:
  // issuer is what we'll have to run if the future fails.
  // it'll expect the transaction to have been reset
  // (which is guaranteed by fdb)
  virtual InflightCallback issue() = 0;

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
  bool suppress_errors = false;

  void start() {
    cb.emplace(issue());
    begin_wait();
  }

 protected:
  // constructor
  Inflight(fuse_req_t, bool, unique_transaction);
  
  void wait_on_future(FDBFuture *, unique_future *);

  std::optional<InflightCallback> cb;

 private:
  // whether we're intended as r/w or not.
  bool readwrite;
  std::queue<FDBFuture *> future_queue;
  void begin_wait();

#if DEBUG
  struct timespec clockstart;
#endif

  friend class InflightAction;
};

class Inflight_markused : public Inflight {
 public:
  Inflight_markused(fuse_req_t, fuse_ino_t, unique_transaction);
  Inflight_markused *reincarnate();
  InflightCallback issue();
 private:
  fuse_ino_t ino;
  unique_future commit;
};

// TODO consider how we might pull the FUSE bits out of these,
// or otherwise abstract what function is being called when one
// of these is 'executed', so that we can swap in a different
// back end for testing, or other fancy things
class InflightAction {
 public:
  static InflightAction BeginWait(InflightCallback newcb) {
    return InflightAction(false, true, false, [newcb](Inflight *i){
	i->cb.emplace(newcb);
      });
  }
  static InflightAction FDBError(fdb_error_t err) {
    // TODO for now we're just going to do the same thing as restart
    // but really we should go into FDB error handling
    return InflightAction(false, false, true, [](Inflight *){ });
  }
  static InflightAction Restart() {
    return InflightAction(false, false, true, [](Inflight *){ });
  }
  static InflightAction None() {
    return InflightAction(true, false, false, [](Inflight *i){
	fuse_reply_none(i->req);
      });
  };
  static InflightAction Ignore() {
    return InflightAction(true, false, false, [](Inflight *i){

      });
  };
  static InflightAction OK() {
    return InflightAction(true, false, false, [](Inflight *i){
	fuse_reply_err(i->req, 0);
      });
  };
  static InflightAction Abort(int err) {
    return InflightAction(true, false, false, [err](Inflight *i) {
	fuse_reply_err(i->req, err);
      });
  }
  static InflightAction Entry(std::shared_ptr<struct fuse_entry_param> e) {
    return InflightAction(true, false, false, [e](Inflight *i) {
	fuse_reply_entry(i->req, e.get());
	if(increment_lookup_count(e->ino)) {
	  // sigh. launch another background transaction to insert the
	  // use record.

	  // TODO this is a potentially correctness-breaking
	  // optimization/simplification, in that this write could
	  // fail, despite us thinking that the inode will continue to
	  // exist as long as we want it.
	  (new Inflight_markused(i->req, e->ino, make_transaction()))->start();
	}
      });
  }
  static InflightAction Attr(std::shared_ptr<struct stat> attr) {
    return InflightAction(true, false, false, [attr](Inflight *i) {
	fuse_reply_attr(i->req, attr.get(), 0.0);
      });
  }
  static InflightAction Buf(std::vector<uint8_t> buf, int actual_size=-1) {
    return InflightAction(true, false, false, [buf, actual_size](Inflight *i) {
	fuse_reply_buf(i->req,
		       reinterpret_cast<const char *>(buf.data()),
		       (actual_size >= 0) ? actual_size : buf.size());
      });
  }
  static InflightAction Readlink(std::string name) {
    return InflightAction(true, false, false, [name](Inflight *i) {
	fuse_reply_readlink(i->req, name.c_str());
      });
  }
  static InflightAction Write(size_t size) {
    return InflightAction(true, false, false, [size](Inflight *i) {
	fuse_reply_write(i->req, size);
      });
  }
protected:
 InflightAction(bool delete_this, bool begin_wait, bool restart,
		std::function<void(Inflight *)> perform)
   : delete_this(delete_this), begin_wait(begin_wait), restart(restart),
    perform(std::move(perform))
  {
  };

  bool delete_this = false;
  bool begin_wait = false;
  bool restart = false;
  std::function<void(Inflight *)> perform;

  friend class Inflight;
};

extern "C" void fdbfs_error_processor(FDBFuture *, void *);
extern "C" void fdbfs_error_checker(FDBFuture *, void *);

#endif // __INFLIGHT_H_
