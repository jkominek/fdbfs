#ifndef __UTIL_H__
#define __UTIL_H__

#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

// Configuration-esque block, to be pulled out later

// #define LZ4_BLOCK_COMPRESSION
#define ZSTD_BLOCK_COMPRESSION

#ifdef LZ4_BLOCK_COMPRESSION
#define BLOCK_COMPRESSION
#endif

#ifdef ZSTD_BLOCK_COMPRESSION
#define BLOCK_COMPRESSION
#endif

#ifdef BLOCK_COMPRESSION
#define SPECIAL_BLOCKS
#endif

// for mode_t
#include <sys/types.h>

#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "values.pb.h"

#define INODE_PREFIX 'i'
#define INODE_USE_PREFIX 0x01
#define DENTRY_PREFIX 'd'
#define DATA_PREFIX 'f'
#define GARBAGE_PREFIX 'g'
#define PID_PREFIX 'p'
#define METADATA_PREFIX 'M'
#define XATTR_NODE_PREFIX 'x'
#define XATTR_DATA_PREFIX 'X'

#define MAXFILENAMELEN 255

// will be filled out before operation begins
extern FDBDatabase *database;
// must NOT be modified after it is set.
extern std::vector<uint8_t> key_prefix;
extern uint8_t BLOCKBITS;
extern uint32_t BLOCKSIZE;
extern int fileblock_prefix_length;
extern int fileblock_key_length;
extern int inode_key_length;
extern int dirent_prefix_length;
extern std::vector<uint8_t> inode_use_identifier;
extern std::unordered_map<fuse_ino_t, uint64_t> lookup_counts;

[[nodiscard]] extern bool increment_lookup_count(fuse_ino_t);
[[nodiscard]] extern bool decrement_lookup_count(fuse_ino_t, uint64_t);

struct fdbfs_filehandle {
  // TODO include a 'noatime' flag, possibly an enum with multiple settings
  // such as: none, normal, rel, lazy.
  // we also need to track the latest atime here, along with whatever Inflight
  // is trying to update the atime. only want to try running one at a time
  // per inode, since we've got to read and then write the inode to update the
  // atime, there's an opportunity for multiple reads to finish, incrementing
  // the atime here before we get around to applying it to fdb. we'll need
  // to be able to lock the value.

  // immutable; if true, update atime field when appropriate.
  bool atime;
  // take before manipulating any of the atime fields
  std::mutex atime_mutex;
  bool atime_update_needed;
  // the time of our most recent access. when updating the database,
  // wait to take the lock and read this for as long as possible.
  struct timespec atime_target;
  // update this whenever we're sure the atime is a larger value.
  // other systems might dramatically increase the atime, at which point
  // we can avoid wasting our time attempting updates.
  struct timespec atime_last_known;
};
[[nodiscard]] extern struct fdbfs_filehandle **
extract_fdbfs_filehandle(struct fuse_file_info *);

[[nodiscard]] extern fuse_ino_t generate_inode();
[[nodiscard]] extern std::vector<uint8_t>
pack_inode_key(fuse_ino_t, char = INODE_PREFIX,
               const std::vector<uint8_t> &suffix = {});
[[nodiscard]] extern std::vector<uint8_t> pack_garbage_key(fuse_ino_t);
[[nodiscard]] extern std::vector<uint8_t>
pack_pid_key(std::vector<uint8_t>, const std::vector<uint8_t> &suffix = {});
[[nodiscard]] extern std::vector<uint8_t> pack_inode_use_key(fuse_ino_t);
[[nodiscard]] extern std::vector<uint8_t>
pack_fileblock_key(fuse_ino_t, uint64_t,
                   const std::vector<uint8_t> &suffix = {});
[[nodiscard]] extern std::vector<uint8_t> pack_dentry_key(fuse_ino_t,
                                                          const std::string &);
[[nodiscard]] extern std::vector<uint8_t>
pack_xattr_key(fuse_ino_t ino, const std::string &name = {});
[[nodiscard]] extern std::vector<uint8_t>
pack_xattr_data_key(fuse_ino_t ino, const std::string &name = {});
extern void print_key(std::vector<uint8_t>);
extern void pack_inode_record_into_stat(const INodeRecord &inode,
                                        struct stat &attr);
template <typename T> void print_bytes(const T *str, int strlength) {
  for (int i = 0; i < strlength; i++) {
    if (isprint(str[i])) {
      printf("%c", str[i]);
    } else {
      printf("\\x%02x", str[i]);
    }
  }
}
using range_keys = std::pair<std::vector<uint8_t>, std::vector<uint8_t>>;
[[nodiscard]] range_keys offset_size_to_range_keys(fuse_ino_t, size_t, size_t);

[[nodiscard]] extern bool
filename_length_check(fuse_req_t, const char *,
                      size_t maxlength = MAXFILENAMELEN);

extern void update_atime(INodeRecord *, const struct timespec *);
extern void update_mtime(INodeRecord *, const struct timespec *);
extern void update_ctime(INodeRecord *, const struct timespec *);
extern void update_directory_times(FDBTransaction *, INodeRecord &);

extern void erase_inode(FDBTransaction *, fuse_ino_t);

[[nodiscard]] extern bool set_block(FDBTransaction *,
                                    const std::vector<uint8_t>, const uint8_t *,
                                    uint64_t, bool = true);
[[nodiscard]] extern int decode_block(const FDBKeyValue *, int, uint8_t *, int,
                                      int);

#ifndef DEBUG
#define DEBUG 0
#endif // DEBUG

#define debug_print(fmt, ...)                                                  \
  do {                                                                         \
    if (DEBUG)                                                                 \
      fprintf(stderr, fmt, __VA_ARGS__);                                       \
  } while (0)

struct FDBTransactionDeleter {
  void operator()(FDBTransaction *t) { fdb_transaction_destroy(t); }
};
struct FDBFutureDeleter {
  void operator()(FDBFuture *f) { fdb_future_destroy(f); }
};
typedef std::unique_ptr<FDBTransaction, FDBTransactionDeleter>
    unique_transaction;
typedef std::unique_ptr<FDBFuture, FDBFutureDeleter> unique_future;

[[nodiscard]] extern unique_transaction make_transaction();
[[nodiscard]] extern unique_future wrap_future(FDBFuture *);

/**
 * This is for simple retryable synchronous transactions, like the
 * ones we'll run in our background threads. It shouldn't be used
 * anywhere performance critical.
 */
template <typename T>
std::optional<T> run_sync_transaction(std::function<T(FDBTransaction *)> f) {
  unique_transaction t = make_transaction();

  while (true) {
    fdb_error_t err = 0;
    T retval;
    try {
      retval = f(t.get());
      unique_future commitfuture = wrap_future(fdb_transaction_commit(t.get()));
      if (fdb_future_block_until_ready(commitfuture.get())) {
        throw new std::runtime_error("failed to block for future");
      }
      if (fdb_future_get_error(commitfuture.get())) {
        err = fdb_future_get_error(commitfuture.get());
      }
    } catch (fdb_error_t _err) {
      err = _err;
    }

    if (!err) {
      return std::make_optional(retval);
    } else {
      unique_future retryfuture =
          wrap_future(fdb_transaction_on_error(t.get(), err));
      if (fdb_future_block_until_ready(retryfuture.get())) {
        throw new std::runtime_error("failed to block for future");
      }
      if (fdb_future_get_error(retryfuture.get())) {
        // failed utterly
        return std::nullopt;
      }
      // fall out and loop
    }
  }
}

#endif // __UTIL_H_
