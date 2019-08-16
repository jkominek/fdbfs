#include "util.h"

#include <strings.h>
#include <stdio.h>
#include <time.h>
#include <ctype.h>

#define max(a,b) \
  ({ __typeof__ (a) _a = (a); \
    __typeof__ (b) _b = (b); \
    _a > _b ? _a : _b; })
#define min(a,b) \
  ({ __typeof__ (a) _a = (a); \
    __typeof__ (b) _b = (b); \
    _a < _b ? _a : _b; })

// will be filled out before operation begins
FDBDatabase *database;
char *kp;
int kplen;

fuse_ino_t generate_inode()
{
  struct timespec tp;
  clock_gettime(CLOCK_REALTIME, &tp);
  // we get ~30 bits from the nanoseconds. we'll leave those
  // in the least significant bytes. the lowest 34 bits of
  // the seconds will become the most significant bytes.
  // since we're going to prefer LSB systems, that'll get the
  // most rapidly varying bytes at the start of the FDB keys,
  // spreading inode allocation uniformly across key space.
  uint64_t h = (tp.tv_sec & 0x3FFFFFFFF) << 30;
  uint64_t l = (tp.tv_nsec & 0x3FFFFFFF) >> 2;
  return (h | l);
}

void pack_inode_key(fuse_ino_t ino, uint8_t *key, int *keylen)
{
  bcopy(kp, key, kplen);
  key[kplen] = INODE_PREFIX;
  bcopy(&ino, key+kplen+1, sizeof(fuse_ino_t));
  *keylen = kplen + 1 + sizeof(fuse_ino_t);
}

void pack_dentry_key(fuse_ino_t ino, char *name, int namelen, uint8_t *key, int *keylen)
{
  pack_inode_key(ino, key, keylen);

  key[*keylen] = DENTRY_PREFIX;
  bcopy(name, key+*keylen+1, namelen);

  *keylen += 1 + namelen;
}

void unpack_stat_from_dbvalue(const uint8_t *val, int vallen, struct stat *attr)
{
  // TODO this isn't a great idea. the stat struct doesn't have to
  // be bytewise identical across kernel versions, let alone
  // microarchitectures or operating systems. we'll have to do
  // something smart later on.
  bzero(attr, sizeof(struct stat));
  bcopy(val, attr, min(sizeof(struct stat), vallen));
  printf("stat struct\n");
  printf("  dev: %li\n", attr->st_dev);
  printf("  ino: %li\n", attr->st_ino);
  printf(" mode: %o\n", attr->st_mode);
  printf("nlink: %li\n", attr->st_nlink);
  printf("  uid: %i\n", attr->st_uid);
  printf("  gid: %i\n", attr->st_gid);
}

void print_bytes(const uint8_t *str, int strlength)
{
  for(int i=0; i<strlength; i++) {
    if(isprint(str[i])) {
      printf("%c", str[i]);
    } else {
      printf("\\x%02x", str[i]);
    }
  }
}
