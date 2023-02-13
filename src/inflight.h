#ifndef __INFLIGHT_H_
#define __INFLIGHT_H_

#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

#include <memory>
#include <deque>
#include <queue>
#include <functional>

#if DEBUG
#include <time.h>
#endif

#include "util.h"

#include "thread_pool.hpp"

// Pool used for processing our callbacks
extern thread_pool pool;


// Halt all inflights and prevent new ones from starting.
extern void shut_it_down();

enum class ReadWrite {
  Yes,
  ReadOnly
};

class InflightAction;

typedef std::function<InflightAction()> InflightCallback;

class Inflight {
 public:
  // issuer is what we'll have to run if the future fails.
  // it'll expect the transaction to have been reset
  // (which is guaranteed by fdb)
  [[nodiscard]] virtual InflightCallback issue() = 0;

  // makes a new fresh version of the inflight object
  // and suicides the old one.
  [[nodiscard]] virtual Inflight *reincarnate() = 0;
  void future_ready(FDBFuture *);

  virtual ~Inflight() = default;
  
  // the transaction we're tied to. will we ever want
  // two separate chains of computation using the same
  // transaction?
  unique_transaction transaction;

  // always need this so that our error processing code
  // can throw errors back to fuse.
  fuse_req_t req;
  bool suppress_errors = false;

  void start();

  InflightAction commit(InflightCallback);

  // run before delete, in case there is anything a subclass
  // wants to take care of.
  void cleanup();

 protected:
  // constructor
  Inflight(fuse_req_t, ReadWrite, unique_transaction);
  
  void wait_on_future(FDBFuture *, unique_future &);

  std::optional<InflightCallback> cb;

  // not used for readonly operations
  unique_future _commit;

private:
  // whether we're intended as r/w or not.
  ReadWrite readwrite;
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
// Make InflightAction a virtual base class, and have, for instance,
// a FUSEInflightAction which is passed in to the function, and it
// can call methods off of that.
class InflightAction {
 public:
  static InflightAction BeginWait(InflightCallback newcb) {
    return InflightAction(false, true, false, [newcb](Inflight *i){
	i->cb.emplace(newcb);
      });
  }
  static InflightAction FDBError(fdb_error_t err) {
    if(fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, err)) {
      // can be retried, do the same thing as Restart
      return InflightAction(false, false, true, [](Inflight *){ });
    } else {
      // can't be retried, surface an error.
      return InflightAction(true, false, false, [](Inflight *i) {
        // is EIO most appropriate?
	fuse_reply_err(i->req, EIO);
      });
    }
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
  static InflightAction Attr(struct stat attr) {
    return InflightAction(true, false, false, [attr](Inflight *i) {
	fuse_reply_attr(i->req, &attr, 0.0);
      });
  }
  static InflightAction Buf(std::vector<uint8_t> buf, int actual_size=-1) {
    // Note, per the default value for actual_size, we might receive
    // a buffer which is larger than the amount of useful/valid data
    // in it. By passing in an actual_size value, calling code can
    // restrict the amount of buf which is passed along.
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
  static InflightAction Statfs(std::shared_ptr<struct statvfs> statbuf) {
    return InflightAction(true, false, false, [statbuf](Inflight *i) {
	 fuse_reply_statfs(i->req, statbuf.get());
      });
  }
  static InflightAction XattrSize(ssize_t size) {
    return InflightAction(true, false, false, [size](Inflight *i) {
						fuse_reply_xattr(i->req, size);
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
