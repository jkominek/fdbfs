#ifndef __UTIL_H__
#define __UTIL_H__

#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 610
#include <foundationdb/fdb_c.h>

// Configuration-esque block, to be pulled out later

//#define LZ4_BLOCK_COMPRESSION
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

#include <vector>
#include <unordered_map>
#include <functional>

#include "values.pb.h"

#define INODE_PREFIX    'i'
#define INODE_USE_PREFIX 0x01
#define DENTRY_PREFIX   'd'
#define DATA_PREFIX     'f'
#define GARBAGE_PREFIX  'g'
#define METADATA_PREFIX 'M'

// will be filled out before operation begins
extern FDBDatabase *database;
extern std::vector<uint8_t> key_prefix;
extern uint8_t BLOCKBITS;
extern uint32_t BLOCKSIZE;
extern int fileblock_prefix_length;
extern int fileblock_key_length;
extern int inode_key_length;
extern int dirent_prefix_length;
extern std::vector<uint8_t> inode_use_identifier;
extern std::unordered_map<fuse_ino_t, uint64_t> lookup_counts;

extern bool increment_lookup_count(fuse_ino_t);
extern bool decrement_lookup_count(fuse_ino_t, uint64_t);

struct fdbfs_filehandle {
};
extern struct fdbfs_filehandle **extract_fdbfs_filehandle(struct fuse_file_info *);

extern fuse_ino_t generate_inode();
extern std::vector<uint8_t> pack_inode_key(fuse_ino_t, char=INODE_PREFIX);
extern std::vector<uint8_t> pack_garbage_key(fuse_ino_t);
extern std::vector<uint8_t> pack_inode_use_key(fuse_ino_t);
extern std::vector<uint8_t> pack_dentry_key(fuse_ino_t, std::string);
extern std::vector<uint8_t> pack_fileblock_key(fuse_ino_t, uint64_t);
extern void print_key(std::vector<uint8_t>);
extern void pack_inode_record_into_stat(INodeRecord *inode, struct stat *attr);
template <typename T>
void print_bytes(const T *str, int strlength)
{
  for(int i=0; i<strlength; i++) {
    if(isprint(str[i])) {
      printf("%c", str[i]);
    } else {
      printf("\\x%02x", str[i]);
    }
  }
}
using range_keys = std::pair<std::vector<uint8_t>, std::vector<uint8_t>>;
range_keys offset_size_to_range_keys(fuse_ino_t, size_t, size_t);

extern bool filename_length_check(fuse_req_t, const char *, size_t maxlength=255);

extern void update_atime(INodeRecord *, struct timespec *);
extern void update_mtime(INodeRecord *, struct timespec *);
extern void update_ctime(INodeRecord *, struct timespec *);
extern void update_directory_times(FDBTransaction *, INodeRecord &);

extern void erase_inode(FDBTransaction *, fuse_ino_t);

extern void set_block(FDBTransaction *, std::vector<uint8_t>,
		      uint8_t *, uint64_t);
extern int decode_block(FDBKeyValue *, int, uint8_t *, int, int);

#ifndef DEBUG
#define DEBUG 0
#endif //DEBUG

#define debug_print(fmt, ...) \
  do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

#endif // __UTIL_H_
