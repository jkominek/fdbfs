#ifndef __UTIL_H__
#define __UTIL_H__

#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 600
#include <foundationdb/fdb_c.h>

#define INODE_PREFIX    'i'
#define DENTRY_PREFIX   'd'
#define DATA_PREFIX     'f'
#define GARBAGE_PREFIX  'g'
#define METADATA_PREFIX 'M'

#define max(a,b) \
  ({ __typeof__ (a) _a = (a); \
    __typeof__ (b) _b = (b); \
    _a > _b ? _a : _b; })
#define min(a,b) \
  ({ __typeof__ (a) _a = (a); \
    __typeof__ (b) _b = (b); \
    _a < _b ? _a : _b; })

// will be filled out before operation begins
extern FDBDatabase *database;
extern char *kp;
extern int kplen;
extern uint8_t BLOCKBITS;
extern uint32_t BLOCKSIZE;

extern void pack_inode_key(fuse_ino_t ino, uint8_t *key, int *keylen);
extern void pack_dentry_key(fuse_ino_t ino, char *name, int namelen, uint8_t *key, int *keylen);
extern void unpack_stat_from_dbvalue(uint8_t *val, int vallen, struct stat *attr);

#ifndef DEBUG
#define DEBUG 0
#endif //DEBUG

#define debug_print(fmt, ...) \
  do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

#endif // __UTIL_H_
