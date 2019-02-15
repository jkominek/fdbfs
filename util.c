#include "util.h"

#include <strings.h>

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

void unpack_stat_from_dbvalue(uint8_t *val, int vallen, struct stat *attr)
{
  // TODO this isn't a great idea. the stat struct doesn't have to
  // be bytewise identical across kernel versions, let alone
  // microarchitectures or operating systems. we'll have to do
  // something smart later on.
  bcopy(val, attr, min(sizeof(struct stat), vallen));
}
