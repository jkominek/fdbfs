#ifndef __UTIL_H__
#define __UTIL_H__

#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 610
#include <foundationdb/fdb_c.h>

// for mode_t
#include <sys/types.h>

#include <vector>

#include "values.pb.h"

#define INODE_PREFIX    'i'
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

extern fuse_ino_t generate_inode();
extern std::vector<uint8_t> pack_inode_key(fuse_ino_t);
extern std::vector<uint8_t> pack_dentry_key(fuse_ino_t, std::string);
extern std::vector<uint8_t> pack_fileblock_key(fuse_ino_t, uint64_t);
extern void print_key(std::vector<uint8_t>);
extern void pack_inode_record_into_stat(INodeRecord *inode, struct stat *attr);
extern void print_bytes(const uint8_t *str, int strlength);

#ifndef DEBUG
#define DEBUG 0
#endif //DEBUG

#define debug_print(fmt, ...) \
  do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

#endif // __UTIL_H_
