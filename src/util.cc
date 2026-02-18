#include "util.h"
#include "liveness.h" // manages pid

#include <assert.h>
#include <ctype.h>
#include <source_location>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <time.h>
#include <tuple>

#ifdef LZ4_BLOCK_COMPRESSION
#include <lz4.h>
#endif

#ifdef ZSTD_BLOCK_COMPRESSION
#include <zstd.h>
#endif

#include "values.pb.h"

namespace {

bool trace_errors_enabled() {
  static const bool enabled = (getenv("FDBFS_TRACE_ERRORS") != nullptr);
  return enabled;
}

template <typename T>
std::expected<T, int>
unexpected_errno(int err, const char *why = "",
                 std::source_location loc = std::source_location::current()) {
  if (trace_errors_enabled()) {
    fprintf(stderr, "fdbfs UtilUnexpected: err=%d (%s) at %s:%u (%s)%s%s\n",
            err, strerror(err), loc.file_name(), loc.line(),
            loc.function_name(), (why && why[0]) ? " why=" : "",
            (why && why[0]) ? why : "");
  }
  return std::unexpected(err);
}

} // namespace

unique_transaction make_transaction() {
  unique_transaction ut;
  FDBTransaction *t;
  if (fdb_database_create_transaction(database, &t)) {
    // this is catastrophic. we can no longer communicate with the database.
    // TODO log a message or something
    std::terminate();
  }
  ut.reset(t);
  return ut;
}

unique_future wrap_future(FDBFuture *f) {
  unique_future uf;
  uf.reset(f);
  return uf;
}

struct fdbfs_filehandle **extract_fdbfs_filehandle(struct fuse_file_info *fi) {
  static_assert(sizeof(fi->fh) >= sizeof(struct fdbfs_filehandle *),
                "FUSE File handle can't hold a pointer to our structure");
  return reinterpret_cast<struct fdbfs_filehandle **>(&(fi->fh));
}

int reply_open_with_handle(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info *fi) {
  struct fdbfs_filehandle *fh = new fdbfs_filehandle;
  fh->atime_update_needed = false;
#ifdef O_NOATIME
  fh->atime = ((fi->flags & O_NOATIME) == 0);
#else
  fh->atime = true;
#endif
  *(extract_fdbfs_filehandle(fi)) = fh;

  auto generation = increment_lookup_count(ino);
  // open should only arrive for an inode that was already looked up.
  assert(!generation.has_value());

  if (fuse_reply_open(req, fi) < 0) {
    // release won't be called if open failed.
    delete fh;

    auto clear_generation = decrement_lookup_count(ino, 1);
    // paired decrement should never drop to zero under the same invariant.
    assert(!clear_generation.has_value());
    return -1;
  }
  return 0;
}

void best_effort_clear_inode_use_record(fuse_ino_t ino, uint64_t generation) {
  auto result = run_sync_transaction<int>([ino, generation](FDBTransaction *t) {
    const auto key = pack_inode_use_key(ino);
    const uint64_t generation_le = htole64(generation);
    fdb_transaction_atomic_op(t, key.data(), key.size(),
                              reinterpret_cast<const uint8_t *>(&generation_le),
                              sizeof(generation_le),
                              FDB_MUTATION_TYPE_COMPARE_AND_CLEAR);
    return 0;
  });
  if (!result.has_value()) {
    // NOTE this is a leaked record. we're going to need to implement
    // some kind of online fsck that can scan all inodes for lingering
    // junk.
    if (trace_errors_enabled()) {
      fprintf(stderr,
              "fdbfs BestEffortClearUseRecord: ino=%llu fdb_err=%d (%s)\n",
              static_cast<unsigned long long>(ino), result.error(),
              fdb_get_error(result.error()));
    }
  }
}

// tracks kernel cache of lookups, so we can avoid fdb
// calls except when we're going from zero to not-zero
// TODO if the FDB networking became multithreaded, or
// we could otherwise process responses in a multithreaded
// fashion, we'd need locking.
std::unordered_map<fuse_ino_t, LookupState> lookup_counts;
std::mutex lookup_counts_mutex;
uint64_t next_lookup_generation = 1;

// if this returns a value, the caller is obligated to
// publish a use record with the generation.
std::optional<uint64_t> increment_lookup_count(fuse_ino_t ino) {
  std::lock_guard<std::mutex> guard(lookup_counts_mutex);
  auto it = lookup_counts.find(ino);
  if (it != lookup_counts.end()) {
    // present
    it->second.count += 1;
    return std::nullopt;
  } else {
    // not present
    if (next_lookup_generation == 0) {
      // wrapped around and reserved value reached. continuing would
      // permit generation reuse and stale clears.
      std::terminate();
    }
    const uint64_t generation = next_lookup_generation;
    next_lookup_generation += 1;
    lookup_counts[ino] = LookupState{1, generation};
    return generation;
  }
}

// if this returns a value, the caller is obligated to
// clear the inode-adjacent record iff generation matches.
std::optional<uint64_t> decrement_lookup_count(fuse_ino_t ino, uint64_t count) {
  std::lock_guard<std::mutex> guard(lookup_counts_mutex);
  auto it = lookup_counts.find(ino);
  if (it == lookup_counts.end()) {
    // well. oops. kernel knew about something that isn't there.
    return std::nullopt;
  } else {
    if (count >= it->second.count) {
      // TODO this should be logged, as it suggests that our
      // counts fell out of sync somehow
      it->second.count = 0;
    } else
      it->second.count -= count;
    if (it->second.count > 0) {
      // still cached, nothing to do.
      return std::nullopt;
    } else {
      const uint64_t generation = it->second.generation;
      // we're forgetting about this inode, drop it
      lookup_counts.erase(ino);
      return generation;
    }
  }
}

bool lookup_count_nonzero(fuse_ino_t ino) {
  std::lock_guard<std::mutex> guard(lookup_counts_mutex);
  auto it = lookup_counts.find(ino);
  if (it == lookup_counts.end()) {
    // record isn't present, so it's zero
    return false;
  } else {
    // record is present, so count should be nonzero
    return it->second.count > 0;
  }
}

// will be filled out before operation begins
std::vector<uint8_t> key_prefix;

int inode_key_length;
std::vector<uint8_t> pack_inode_key(fuse_ino_t _ino, char prefix,
                                    const std::vector<uint8_t> &suffix) {
  auto key = key_prefix;
  key.push_back(prefix);

  const fuse_ino_t ino = htobe64(_ino);
  const auto tmp = reinterpret_cast<const uint8_t *>(&ino);
  key.insert(key.end(), tmp, tmp + sizeof(fuse_ino_t));

  key.insert(key.end(), suffix.begin(), suffix.end());
  return key;
}

std::vector<uint8_t> pack_garbage_key(fuse_ino_t ino) {
  return pack_inode_key(ino, GARBAGE_PREFIX);
}

std::vector<uint8_t> pack_pid_key(std::vector<uint8_t> p,
                                  const std::vector<uint8_t> &suffix) {
  auto key = key_prefix;
  key.push_back(PID_PREFIX);
  key.insert(key.end(), p.begin(), p.end());

  key.insert(key.end(), suffix.begin(), suffix.end());
  return key;
}

std::vector<uint8_t> pack_oplog_key(const std::vector<uint8_t> &owner_pid,
                                    uint64_t op_id) {
  auto key = key_prefix;
  key.push_back('o');
  key.insert(key.end(), owner_pid.begin(), owner_pid.end());

  const uint64_t op_id_be = htobe64(op_id);
  const auto *tmp = reinterpret_cast<const uint8_t *>(&op_id_be);
  key.insert(key.end(), tmp, tmp + sizeof(op_id_be));
  return key;
}

std::vector<uint8_t> pack_inode_use_key(fuse_ino_t ino) {
  auto key = pack_inode_key(ino);
  key.push_back(INODE_USE_PREFIX);
  key.insert(key.end(), pid.begin(), pid.end());
  return key;
}

int fileblock_prefix_length;
int fileblock_key_length;
std::vector<uint8_t> pack_fileblock_key(fuse_ino_t ino, uint64_t _block,
                                        const std::vector<uint8_t> &suffix) {
  auto key = pack_inode_key(ino, DATA_PREFIX);

  // TODO this is fast on our end, but every file block key now has 64
  // bits in it, where most of those 64 bits will be 0, most of the
  // time. which are stored redundantly and moved back and forth across
  // the network on a regular basis.
  // most OSes are going to limit us to 64 bit files anyways. so we
  // really only need to go up to (64 - BLOCKBITS) here.
  // so we should consider switching over to a variable length representation
  // of the block number. we could even imagine replacing the DATA_PREFIX
  // 'f' with the values 0xF8 through 0xFF, and then taking the lowest
  // three bits as representing the number of bytes in the block number
  // representation. (because 2^3 bytes for representing an integer is
  // plenty.)

  const auto block = htobe64(_block);
  const auto tmpp = reinterpret_cast<const uint8_t *>(&block);
  key.insert(key.end(), tmpp, tmpp + sizeof(uint64_t));

  key.insert(key.end(), suffix.begin(), suffix.end());
  return key;
}

int dirent_prefix_length;
std::vector<uint8_t> pack_dentry_key(fuse_ino_t ino, const std::string &name) {
  auto key = pack_inode_key(ino, DENTRY_PREFIX);

  key.insert(key.end(), name.begin(), name.end());
  return key;
}

std::vector<uint8_t> pack_xattr_key(fuse_ino_t ino, const std::string &name) {
  auto key = pack_inode_key(ino, XATTR_NODE_PREFIX);

  key.insert(key.end(), name.begin(), name.end());
  return key;
}

std::vector<uint8_t> pack_xattr_data_key(fuse_ino_t ino,
                                         const std::string &name) {
  auto key = pack_inode_key(ino, XATTR_DATA_PREFIX);

  key.insert(key.end(), name.begin(), name.end());
  return key;
}

std::vector<uint8_t> prefix_range_end(const std::vector<uint8_t> &prefix) {
  auto stop = prefix;
  for (size_t i = stop.size(); i > 0; --i) {
    if (stop[i - 1] != 0xff) {
      stop[i - 1] += 1;
      stop.resize(i);
      return stop;
    }
  }
  // our keys always start with key_prefix, so this should be unreachable.
  std::terminate();
}

// all process-table records for this filesystem prefix.
// [ key_prefix + PID_PREFIX, key_prefix + (PID_PREFIX + 1) ).
range_keys pack_pid_subspace_range() {
  auto start = key_prefix;
  start.push_back(PID_PREFIX);
  auto stop = key_prefix;
  stop.push_back(PID_PREFIX + 1);
  assert(start < stop);
  return {start, stop};
}

// one process-table record namespace (base PID key plus any suffixes).
// [ pack_pid_key(record_pid), prefix_range_end(pack_pid_key(record_pid)) ).
range_keys pack_pid_record_range(const std::vector<uint8_t> &record_pid) {
  auto start = pack_pid_key(record_pid);
  auto stop = prefix_range_end(start);
  assert(start < stop);
  return {start, stop};
}

// all oplog records for a specific owner pid.
// [ key_prefix + 'o' + owner_pid,
//   prefix_range_end(key_prefix + 'o' + owner_pid) ).
range_keys pack_oplog_subspace_range(const std::vector<uint8_t> &owner_pid) {
  auto start = key_prefix;
  start.push_back('o');
  start.insert(start.end(), owner_pid.begin(), owner_pid.end());
  auto stop = prefix_range_end(start);
  assert(start < stop);
  return {start, stop};
}

// local process oplog span.
// [ pack_oplog_key(pid, start_op_id), pack_oplog_key(pid, stop_op_id) ).
range_keys pack_local_oplog_span_range(uint64_t start_op_id,
                                       uint64_t stop_op_id) {
  assert(start_op_id < stop_op_id);
  auto start = pack_oplog_key(pid, start_op_id);
  auto stop = pack_oplog_key(pid, stop_op_id);
  assert(start < stop);
  return {start, stop};
}

// all garbage-collection marker records for this filesystem prefix.
// [ key_prefix + GARBAGE_PREFIX, key_prefix + (GARBAGE_PREFIX + 1) ).
range_keys pack_garbage_subspace_range() {
  auto start = key_prefix;
  start.push_back(GARBAGE_PREFIX);
  auto stop = key_prefix;
  stop.push_back(GARBAGE_PREFIX + 1);
  assert(start < stop);
  return {start, stop};
}

// metadata keys for one inode (metadata, lock, use records).
// [ pack_inode_key(ino), pack_inode_key(ino + 1) ) with max-ino fallback.
range_keys pack_inode_subspace_range(fuse_ino_t ino) {
  auto start = pack_inode_key(ino);
  std::vector<uint8_t> stop;
  if (ino != std::numeric_limits<fuse_ino_t>::max()) {
    stop = pack_inode_key(ino + 1);
  } else {
    stop = key_prefix;
    stop.push_back(INODE_PREFIX + 1);
  }
  assert(start < stop);
  return {start, stop};
}

// all inode-use records for one inode (across all hosts/pids).
// [ pack_inode_key(ino, INODE_PREFIX, {INODE_USE_PREFIX}),
//   pack_inode_key(ino, INODE_PREFIX, {INODE_USE_PREFIX + 1}) ).
range_keys pack_inode_use_subspace_range(fuse_ino_t ino) {
  auto start = pack_inode_key(ino, INODE_PREFIX, {INODE_USE_PREFIX});
  auto stop = pack_inode_key(ino, INODE_PREFIX, {INODE_USE_PREFIX + 1});
  assert(start < stop);
  return {start, stop};
}

// inode metadata key and its adjacent use-record subspace.
// [ pack_inode_key(ino), pack_inode_key(ino, INODE_PREFIX, {0x02}) ).
range_keys pack_inode_metadata_and_use_range(fuse_ino_t ino) {
  auto start = pack_inode_key(ino);
  auto stop = pack_inode_key(ino, INODE_PREFIX, {0x02});
  assert(start < stop);
  return {start, stop};
}

// Even if we want to fetch a single block, we've got to use a range,
// because we encode information like (optional) compression algorithm
// or parity block info at the tail end of the key.
//
// all physical key variants for one logical file block.
// [ pack_fileblock_key(ino, block), pack_fileblock_key(ino, block + 1) )
// with max-block fallback into the next inode or next top-level subspace.
range_keys pack_fileblock_single_range(fuse_ino_t ino, uint64_t block) {
  auto start = pack_fileblock_key(ino, block);
  std::vector<uint8_t> stop;
  if (block != std::numeric_limits<uint64_t>::max()) {
    stop = pack_fileblock_key(ino, block + 1);
  } else if (ino != std::numeric_limits<fuse_ino_t>::max()) {
    stop = pack_fileblock_key(ino + 1, 0);
  } else {
    stop = key_prefix;
    stop.push_back(DATA_PREFIX + 1);
  }
  assert(start < stop);
  return {start, stop};
}

// all data blocks for one inode, from start_block through stop_block.
// [ pack_fileblock_key(ino, start_block),
//   pack_fileblock_key(ino, stop_block + 1) ) with max-block fallback.
range_keys pack_fileblock_span_range(fuse_ino_t ino, uint64_t start_block,
                                     uint64_t stop_block) {
  auto start = pack_fileblock_key(ino, start_block);
  std::vector<uint8_t> stop;
  if (stop_block != std::numeric_limits<uint64_t>::max()) {
    stop = pack_fileblock_key(ino, stop_block + 1);
  } else if (ino != std::numeric_limits<fuse_ino_t>::max()) {
    stop = pack_fileblock_key(ino + 1, 0);
  } else {
    stop = key_prefix;
    stop.push_back(DATA_PREFIX + 1);
  }
  assert(start < stop);
  return {start, stop};
}

// all directory-entry keys for one parent inode.
// [ pack_inode_key(ino, DENTRY_PREFIX),
//   pack_inode_key(ino + 1, DENTRY_PREFIX) ) with max-ino fallback.
range_keys pack_dentry_subspace_range(fuse_ino_t ino) {
  auto start = pack_inode_key(ino, DENTRY_PREFIX);
  std::vector<uint8_t> stop;
  if (ino != std::numeric_limits<fuse_ino_t>::max()) {
    stop = pack_inode_key(ino + 1, DENTRY_PREFIX);
  } else {
    stop = key_prefix;
    stop.push_back(DENTRY_PREFIX + 1);
  }
  assert(start < stop);
  return {start, stop};
}

// all xattr-node (name/value-pointer) keys for one inode.
// [ pack_inode_key(ino, XATTR_NODE_PREFIX),
//   pack_inode_key(ino + 1, XATTR_NODE_PREFIX) ) with max-ino fallback.
range_keys pack_xattr_node_subspace_range(fuse_ino_t ino) {
  auto start = pack_inode_key(ino, XATTR_NODE_PREFIX);
  std::vector<uint8_t> stop;
  if (ino != std::numeric_limits<fuse_ino_t>::max()) {
    stop = pack_inode_key(ino + 1, XATTR_NODE_PREFIX);
  } else {
    stop = key_prefix;
    stop.push_back(XATTR_NODE_PREFIX + 1);
  }
  assert(start < stop);
  return {start, stop};
}

// all xattr-data payload keys for one inode.
// [ pack_inode_key(ino, XATTR_DATA_PREFIX),
//   pack_inode_key(ino + 1, XATTR_DATA_PREFIX) ) with max-ino fallback.
range_keys pack_xattr_data_subspace_range(fuse_ino_t ino) {
  auto start = pack_inode_key(ino, XATTR_DATA_PREFIX);
  std::vector<uint8_t> stop;
  if (ino != std::numeric_limits<fuse_ino_t>::max()) {
    stop = pack_inode_key(ino + 1, XATTR_DATA_PREFIX);
  } else {
    stop = key_prefix;
    stop.push_back(XATTR_DATA_PREFIX + 1);
  }
  assert(start < stop);
  return {start, stop};
}

void print_key(std::vector<uint8_t> v) {
  printf("%zu ", v.size());
  for (std::vector<uint8_t>::const_iterator i = v.begin(); i != v.end(); ++i)
    if (isprint(*i))
      printf("%c", *i);
    else
      printf("\\x%02x", *i);
  printf("\n");
}

void pack_inode_record_into_stat(const INodeRecord &inode, struct stat &attr) {
  attr.st_ino = inode.inode();
  attr.st_dev = 0;
  attr.st_mode = inode.mode() | inode.type();
  attr.st_nlink = inode.nlinks();
  if (inode.has_uid())
    attr.st_uid = inode.uid();
  else
    attr.st_uid = 0;

  if (inode.has_gid())
    attr.st_gid = inode.gid();
  else
    attr.st_gid = 0;

  if (inode.has_size())
    attr.st_size = inode.size();
  else
    attr.st_size = 0;

  if (inode.has_atime()) {
    attr.st_atim.tv_sec = inode.atime().sec();
    attr.st_atim.tv_nsec = inode.atime().nsec();
  }

  if (inode.has_mtime()) {
    attr.st_mtim.tv_sec = inode.mtime().sec();
    attr.st_mtim.tv_nsec = inode.mtime().nsec();
  }

  if (inode.has_ctime()) {
    attr.st_ctim.tv_sec = inode.ctime().sec();
    attr.st_ctim.tv_nsec = inode.ctime().nsec();
  }

  attr.st_blksize = BLOCKSIZE;
  attr.st_blocks = (attr.st_size / 512) + 1;

  /*
  printf("stat struct\n");
  printf("  dev: %li\n", attr.st_dev);
  printf("  ino: %li\n", attr.st_ino);
  printf(" mode: %o\n", attr.st_mode);
  printf("nlink: %li\n", attr.st_nlink);
  printf("  uid: %i\n", attr.st_uid);
  printf("  gid: %i\n", attr.st_gid);
  */
}

StatRecord pack_stat_into_stat_record(const struct stat &attr) {
  StatRecord record;
  record.set_ino(attr.st_ino);
  record.set_dev(attr.st_dev);
  record.set_mode(attr.st_mode);
  record.set_nlink(attr.st_nlink);
  record.set_uid(attr.st_uid);
  record.set_gid(attr.st_gid);
  record.set_size(attr.st_size);
  record.mutable_atime()->set_sec(attr.st_atim.tv_sec);
  record.mutable_atime()->set_nsec(attr.st_atim.tv_nsec);
  record.mutable_mtime()->set_sec(attr.st_mtim.tv_sec);
  record.mutable_mtime()->set_nsec(attr.st_mtim.tv_nsec);
  record.mutable_ctime()->set_sec(attr.st_ctim.tv_sec);
  record.mutable_ctime()->set_nsec(attr.st_ctim.tv_nsec);
  record.set_blksize(attr.st_blksize);
  record.set_blocks(attr.st_blocks);
  return record;
}

void unpack_stat_record_into_stat(const StatRecord &record, struct stat &attr) {
  attr = {};
  attr.st_ino = record.ino();
  attr.st_dev = record.dev();
  attr.st_mode = record.mode();
  attr.st_nlink = record.nlink();
  attr.st_uid = record.uid();
  attr.st_gid = record.gid();
  attr.st_size = record.size();
  attr.st_atim.tv_sec = record.atime().sec();
  attr.st_atim.tv_nsec = record.atime().nsec();
  attr.st_mtim.tv_sec = record.mtime().sec();
  attr.st_mtim.tv_nsec = record.mtime().nsec();
  attr.st_ctim.tv_sec = record.ctime().sec();
  attr.st_ctim.tv_nsec = record.ctime().nsec();
  attr.st_blksize = record.blksize();
  attr.st_blocks = record.blocks();
}

range_keys offset_size_to_range_keys(fuse_ino_t ino, size_t off, size_t size) {
  uint64_t start_block = off >> BLOCKBITS;
  uint64_t stop_block = ((off + size - 1) >> BLOCKBITS);
  return pack_fileblock_span_range(ino, start_block, stop_block);
}

bool filename_length_check(fuse_req_t req, const char *name, size_t maxlength) {
  if (strnlen(name, maxlength + 1) > maxlength) {
    fuse_reply_err(req, ENAMETOOLONG);
    return true;
  }
  return false;
}

void update_atime(INodeRecord *inode, const struct timespec *tv) {
  Timespec *atime = inode->mutable_atime();
  atime->set_sec(tv->tv_sec);
  atime->set_nsec(tv->tv_nsec);
}

void update_ctime(INodeRecord *inode, const struct timespec *tv) {
  Timespec *ctime = inode->mutable_ctime();
  ctime->set_sec(tv->tv_sec);
  ctime->set_nsec(tv->tv_nsec);
  update_atime(inode, tv);
}

void update_mtime(INodeRecord *inode, const struct timespec *tv) {
  Timespec *mtime = inode->mutable_mtime();
  mtime->set_sec(tv->tv_sec);
  mtime->set_nsec(tv->tv_nsec);
  update_ctime(inode, tv);
}

void update_directory_times(FDBTransaction *transaction, INodeRecord &inode) {
  struct timespec tp;
  clock_gettime(CLOCK_REALTIME, &tp);
  inode.mutable_ctime()->set_sec(tp.tv_sec);
  inode.mutable_ctime()->set_nsec(tp.tv_nsec);
  inode.mutable_mtime()->set_sec(tp.tv_sec);
  inode.mutable_mtime()->set_nsec(tp.tv_nsec);
  // discard the error; failure shouldn't be possible and even if it does
  // somehow happen, failing to update directory times is minor.
  (void)fdb_set_protobuf(transaction, pack_inode_key(inode.inode()), inode);
}

void erase_inode(FDBTransaction *transaction, fuse_ino_t ino) {
  // inode data
  fdbfs_transaction_clear_range(transaction, pack_inode_subspace_range(ino));

  // TODO be clever and only issue these clears based on inode type

  // file data
  fdbfs_transaction_clear_range(transaction,
                                pack_fileblock_span_range(ino, 0, UINT64_MAX));
  // directory listing
  fdbfs_transaction_clear_range(transaction, pack_dentry_subspace_range(ino));
  // xattr nodes
  fdbfs_transaction_clear_range(transaction,
                                pack_xattr_node_subspace_range(ino));
  // xattr data
  fdbfs_transaction_clear_range(transaction,
                                pack_xattr_data_subspace_range(ino));
}

inline void sparsify(const uint8_t *block, size_t *write_size) {
  // sparsify our writes, by truncating nulls from the end of
  // blocks, and just clearing away totally null blocks
  for (; *(write_size) > 0; *(write_size) -= 1) {
    if (block[*(write_size)-1] != 0x00)
      break;
  }
}

std::expected<void, int> set_block(FDBTransaction *transaction,
                                   const std::vector<uint8_t> &key,
                                   std::span<const uint8_t> block,
                                   bool write_conflict) {
  if (key.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
    return std::unexpected(EOVERFLOW);
  const int key_size = static_cast<int>(key.size());

  size_t size = block.size();
  sparsify(block.data(), &size);
  if (size > 0) {
    if (!write_conflict)
      if (fdb_transaction_set_option(
              transaction, FDB_TR_OPTION_NEXT_WRITE_NO_WRITE_CONFLICT_RANGE,
              NULL, 0))
        /* it doesn't matter if this fails. semantics will be preserved,
           there will just be some performance loss. */
        ;

// TODO here's where we'd implement the write-side cleverness for our
// block encoding schemes. they should all not only be ifdef'd, but
// check for whether or not the feature is enabled on the filesystem.
#ifdef BLOCK_COMPRESSION
    if (size >= 64) {
      // considering that these blocks may be stored 3 times, and over
      // their life may have to be moved repeatedly across WANs between
      // data centers, we'll accept very small amounts of compression:
      const size_t acceptable_size = BLOCKSIZE - 16;
      std::vector<uint8_t> compressed(BLOCKSIZE);
// we're arbitrarily saying blocks should be at least 64 bytes
// after sparsification, before we'll attempt to compress them.
#ifdef ZSTD_BLOCK_COMPRESSION
      const size_t ret =
          ZSTD_compress(reinterpret_cast<void *>(compressed.data()), BLOCKSIZE,
                        reinterpret_cast<const void *>(block.data()), size, 10);
      if ((!ZSTD_isError(ret)) && (ret <= acceptable_size)) {
        // ok, we'll take it.
        auto compkey = key;
        compkey.push_back('z');  // compressed
        compkey.push_back(0x01); // 1 byte of arguments
        compkey.push_back(0x01); // ZSTD marker
        if (compkey.size() >
            static_cast<size_t>(std::numeric_limits<int>::max()))
          return std::unexpected(EOVERFLOW);
        const int compkey_size = static_cast<int>(compkey.size());
        if (ret > static_cast<size_t>(std::numeric_limits<int>::max()))
          return std::unexpected(EOVERFLOW);
        fdb_transaction_set(transaction, compkey.data(), compkey_size,
                            compressed.data(), static_cast<int>(ret));
        return {};
      }
#endif
    }
#endif
    // we'll fall back to this if none of the compression schemes bail out
    if (size > static_cast<size_t>(std::numeric_limits<int>::max()))
      return std::unexpected(EOVERFLOW);
    fdb_transaction_set(transaction, key.data(), key_size, block.data(),
                        static_cast<int>(size));
    return {};
  } else {
    // storage model allows for sparsity; interprets missing blocks as nulls.
    // clear any stale data for this block key.
    fdb_transaction_clear(transaction, key.data(), key_size);
    return {};
  }
}

/**
 * Given a block's KV pair, decode it into output, up to targetsize and
 * no further than output.size().
 *
 * raw value:    |ccccccccccccccccccccccccccccccccc|
 *                                                 ^ kv.value_length
 * decompressed: |ddddddddddddddddddddddddddddddddddddddddddd|
 *                                                           output.size() ^
 *                                        target_size v
 * needed:                       |--------------------|
 *                               ^ value_offset
 *
 * the expected value is the number of bytes decoded
 * the unexpected value is some mess of error codes.
 */
std::expected<size_t, int> decode_block(const FDBKeyValue *kv, int block_offset,
                                        std::span<uint8_t> output,
                                        size_t targetsize) {
  if (block_offset < 0)
    return std::unexpected(EINVAL);

  const uint8_t *key = kv->key;
  const uint8_t *value = kv->value;
  const size_t bounded_target = std::min(targetsize, output.size());
  // printf("decoding block\n");
  // print_bytes(value, kv->value_length);printf("\n");
  if (kv->key_length == fileblock_key_length) {
    // printf("   plain.\n");
    //  plain block. there's no added info after the block key
    if (block_offset >= kv->value_length)
      return 0;
    const size_t available =
        kv->value_length - static_cast<size_t>(block_offset);
    const size_t amount = std::min(available, bounded_target);
    if (amount > 0)
      bcopy(value + block_offset, output.data(), amount);
    return amount;
  }

#ifdef SPECIAL_BLOCKS
  // printf("   not plain!\n");
  //  ah! not a plain block! there might be something interesting!
  //  ... for now we just support compression
  const int i = fileblock_key_length;

#ifdef BLOCK_COMPRESSION
  if (key[i] == 'z') {
    // printf("   compressed\n");
    const int arglen = key[i + 1];
    if (arglen <= 0) {
      // printf("    no arg\n");
      //  no argument, but we needed to know compression type
      return unexpected_errno<size_t>(
          EIO, "compressed block missing compression argument");
    }

#ifdef LZ4_BLOCK_COMPRESSION
    // for now we only know how to interpret a single byte of argument
    if (key[i + 2] == 0x00) {
      // 0x00 means LZ4

      std::vector<char> buffer(BLOCKSIZE);
      // we'll only ask that enough be decompressed to satisfy the request
      const int ret = LZ4_decompress_safe_partial(
          reinterpret_cast<const char *>(value), buffer.data(),
          kv->value_length, BLOCKSIZE, BLOCKSIZE);

      if (ret < 0) {
        return std::unexpected(ret);
      }
      if (ret > block_offset) {
        // decompression produced at least one byte worth sending back
        const size_t amount =
            std::min(static_cast<size_t>(ret - block_offset), bounded_target);
        bcopy(buffer.data() + block_offset, output.data(), amount);
        return amount;
      } else {
        // there was less data in the block than necessary to reach the
        // start of the copy, so we don't have to do anything.
        return 0;
      }
    }
#endif

#ifdef ZSTD_BLOCK_COMPRESSION
    if (key[i + 2] == 0x01) {
      // 0x01 means ZSTD
      std::vector<uint8_t> buffer(BLOCKSIZE);
      const size_t ret =
          ZSTD_decompress(buffer.data(), BLOCKSIZE, value, kv->value_length);
      if (ZSTD_isError(ret)) {
        // error
        return unexpected_errno<size_t>(EIO, "ZSTD decompression failed");
      }

      if (ret > static_cast<size_t>(block_offset)) {
        const size_t amount =
            std::min(ret - static_cast<size_t>(block_offset), bounded_target);
        bcopy(buffer.data() + block_offset, output.data(), amount);
        return amount;
      } else {
        // nothing to copy
        return 0;
      }
    }
#endif
    // unrecognized compression algorithm
    return unexpected_errno<size_t>(EIO,
                                    "compressed block has unknown algorithm");
  }
#endif

#endif

  // unrecognized block type.
  return unexpected_errno<size_t>(EIO, "unknown block type");
}

std::expected<void, int>
fdb_set_protobuf(FDBTransaction *tx, const std::vector<uint8_t> &key,
                 const google::protobuf::MessageLite &msg) {
  if (key.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
    return std::unexpected(EOVERFLOW);

  const size_t n = msg.ByteSizeLong();
  if (n > static_cast<size_t>(std::numeric_limits<int>::max()))
    return std::unexpected(EOVERFLOW);

  std::vector<uint8_t> buf(n);
  if (!msg.SerializeToArray(buf.data(), static_cast<int>(n)))
    return unexpected_errno<void>(EIO, "protobuf serialization failed");

  fdb_transaction_set(tx, key.data(), static_cast<int>(key.size()), buf.data(),
                      static_cast<int>(n));

  return {};
}
