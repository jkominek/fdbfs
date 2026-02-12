#include "util.h"
#include "liveness.h" // manages pid

#include <ctype.h>
#include <stdio.h>
#include <strings.h>
#include <time.h>

#ifdef LZ4_BLOCK_COMPRESSION
#include <lz4.h>
#endif

#ifdef ZSTD_BLOCK_COMPRESSION
#include <zstd.h>
#endif

#include "values.pb.h"

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

// tracks kernel cache of lookups, so we can avoid fdb
// calls except when we're going from zero to not-zero
// TODO if the FDB networking became multithreaded, or
// we could otherwise process responses in a multithreaded
// fashion, we'd need locking.
std::unordered_map<fuse_ino_t, uint64_t> lookup_counts;
std::mutex lookup_counts_mutex;

// if this returns true, the caller is obligated to
// insert a record adjacent to the inode to keep it alive
bool increment_lookup_count(fuse_ino_t ino) {
  std::lock_guard<std::mutex> guard(lookup_counts_mutex);
  auto it = lookup_counts.find(ino);
  if (it != lookup_counts.end()) {
    // present
    it->second += 1;
    return false;
  } else {
    // not present
    lookup_counts[ino] = 1;
    return true;
  }
}

// if this returns true, the caller is obligated to
// remove the inode adjacent record that keeps it alive
bool decrement_lookup_count(fuse_ino_t ino, uint64_t count) {
  std::lock_guard<std::mutex> guard(lookup_counts_mutex);
  auto it = lookup_counts.find(ino);
  if (it == lookup_counts.end()) {
    // well. oops. kernel knew about something that isn't there.
    return false;
  } else {
    if (count >= it->second) {
      // TODO this should be logged, as it suggests that our
      // counts fell out of sync somehow
      it->second = 0;
    } else
      it->second -= count;
    if (it->second > 0) {
      // still cached, nothing to do.
      return false;
    } else {
      // we're forgetting about this inode, drop it
      lookup_counts.erase(ino);
      return true;
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
    // record is present, so it's nonzero
    return true;
  }
}

// will be filled out before operation begins
std::vector<uint8_t> key_prefix;

fuse_ino_t generate_inode() {
  // TODO everything that uses this will need to check that
  // it isn't trampling an existing inode.
  struct timespec tp;
  clock_gettime(CLOCK_REALTIME, &tp);
  // we get 30 bits from the nanoseconds. we'll move
  // those up to the high end of what will be the key.
  // the low 34 bit of the seconds will be moved to
  // the low end of the key.
  uint64_t l = (tp.tv_sec & 0x3FFFFFFFF);
  uint64_t h = (tp.tv_nsec & 0x3FFFFFFF) << 34;

  // returning MAX_UINT64 would probably be bad, because i'm
  // not convinced we've correctly covered all of the edge
  // cases around that. but considering our generation method,
  // i don't think it'll be a concern
  return (h | l);
}

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

range_keys offset_size_to_range_keys(fuse_ino_t ino, size_t off, size_t size) {
  uint64_t start_block = off >> BLOCKBITS;
  uint64_t stop_block = ((off + size - 1) >> BLOCKBITS);
  auto start = pack_fileblock_key(ino, start_block);
  auto stop = pack_fileblock_key(ino, stop_block);
  stop.push_back(0xff);
  return std::pair(start, stop);
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
  auto key_start = pack_inode_key(ino);
  auto key_stop = key_start;
  key_stop.push_back('\xff');
  fdb_transaction_clear_range(transaction, key_start.data(), key_start.size(),
                              key_stop.data(), key_stop.size());

  // TODO be clever and only isse these clears based on inode type
  // file data
  key_start = pack_fileblock_key(ino, 0);
  key_stop = pack_fileblock_key(ino, UINT64_MAX);
  key_stop.push_back('\xff');
  fdb_transaction_clear_range(transaction, key_start.data(), key_start.size(),
                              key_stop.data(), key_stop.size());

  // directory listing
  key_start = pack_dentry_key(ino, "");
  key_stop = pack_dentry_key(ino, "\xff");
  fdb_transaction_clear_range(transaction, key_start.data(), key_start.size(),
                              key_stop.data(), key_stop.size());

  // xattr nodes
  key_start = pack_xattr_key(ino, "");
  key_stop = pack_xattr_key(ino, "\xff");
  fdb_transaction_clear_range(transaction, key_start.data(), key_start.size(),
                              key_stop.data(), key_stop.size());

  // xattr data
  key_start = pack_xattr_data_key(ino, "");
  key_stop = pack_xattr_data_key(ino, "\xff");
  fdb_transaction_clear_range(transaction, key_start.data(), key_start.size(),
                              key_stop.data(), key_stop.size());
}

inline void sparsify(const uint8_t *block, uint64_t *write_size) {
  // sparsify our writes, by truncating nulls from the end of
  // blocks, and just clearing away totally null blocks
  for (; *(write_size) > 0; *(write_size) -= 1) {
    if (block[*(write_size)-1] != 0x00)
      break;
  }
}

/* returning false means that we've actually queued a write to the block
 * returning true means that the block can be left empty, due to sparsity.
 * this is so that you can write
 * if(set_block(...)) { fdb_transaction_clear(...) }
 */
bool set_block(FDBTransaction *transaction, const std::vector<uint8_t> key,
               const uint8_t *buffer, uint64_t size, bool write_conflict) {
  sparsify(buffer, &size);
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
      const int acceptable_size = BLOCKSIZE - 16;
      uint8_t compressed[BLOCKSIZE];
// we're arbitrarily saying blocks should be at least 64 bytes
// after sparsification, before we'll attempt to compress them.
#ifdef ZSTD_BLOCK_COMPRESSION
      const int ret =
          ZSTD_compress(reinterpret_cast<void *>(compressed), BLOCKSIZE,
                        reinterpret_cast<const void *>(buffer), size, 10);
      if ((!ZSTD_isError(ret)) && (ret <= acceptable_size)) {
        // ok, we'll take it.
        auto compkey = key;
        compkey.push_back('z');  // compressed
        compkey.push_back(0x01); // 1 byte of arguments
        compkey.push_back(0x01); // ZSTD marker
        fdb_transaction_set(transaction, compkey.data(), compkey.size(),
                            compressed, ret);
        return false;
      }
#endif
    }
#endif
    // we'll fall back to this if none of the compression schemes bail out
    fdb_transaction_set(transaction, key.data(), key.size(), buffer, size);
    return false;
  } else {
    // storage model allows for sparsity; interprets missing blocks as nulls
    // caller may need to remove the block if we return false.
    return true;
  }
}

/**
 * Given a block's KV pair, decode it into output, preferably to targetsize,
 * but definitely no further than maxsize.
 * return negative for error; positive for length
 *
 * raw value:    |ccccccccccccccccccccccccccccccccc|
 *                                                 ^ kv.value_length
 * decompressed: |ddddddddddddddddddddddddddddddddddddddddddd|
 *                                                           maxsize ^
 *                                        target_size v
 * needed:                       |--------------------|
 *                               ^ value_offset
 *
 */
int decode_block(const FDBKeyValue *kv, int block_offset, uint8_t *output,
                 int targetsize, int maxsize) {
  const uint8_t *key = kv->key;
  const uint8_t *value = kv->value;
  // printf("decoding block\n");
  // print_bytes(value, kv->value_length);printf("\n");
  if (kv->key_length == fileblock_key_length) {
    // printf("   plain.\n");
    //  plain block. there's no added info after the block key
    const int amount = std::min(kv->value_length - block_offset, maxsize);
    if (amount > 0) {
      bcopy(value + block_offset, output, amount);
      return amount;
    } else {
      return 0;
    }
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
      return -1;
    }

#ifdef LZ4_BLOCK_COMPRESSION
    // for now we only know how to interpret a single byte of argument
    if (key[i + 2] == 0x00) {
      // 0x00 means LZ4

      char buffer[BLOCKSIZE];
      // we'll only ask that enough be decompressed to satisfy the request
      const int ret = LZ4_decompress_safe_partial(
          value, buffer, kv->value_length, BLOCKSIZE, BLOCKSIZE);
      printf("%i\n", ret);
      if (ret < 0) {
        return ret;
      }
      if (ret > block_offset) {
        // decompression produced at least one byte worth sending back
        const int amount = std::min(ret - block_offset, targetsize);
        bcopy(buffer + block_offset, output, amount);
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
      uint8_t buffer[BLOCKSIZE];
      const int ret =
          ZSTD_decompress(buffer, BLOCKSIZE, value, kv->value_length);
      if (ZSTD_isError(ret)) {
        // error
        return -1;
      }

      if (ret > block_offset) {
        const int amount = std::min(ret - block_offset, targetsize);
        bcopy(buffer + block_offset, output, amount);
        return amount;
      } else {
        // nothing to copy
        return 0;
      }
    }
#endif
    // unrecognized compression algorithm
    return -1;
  }
#endif

#endif

  // unrecognized block type.
  return -1;
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
    return std::unexpected(EIO);

  fdb_transaction_set(tx, key.data(), static_cast<int>(key.size()), buf.data(),
                      static_cast<int>(n));

  return {};
}
