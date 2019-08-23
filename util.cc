#include "util.h"

#include <strings.h>
#include <stdio.h>
#include <time.h>
#include <ctype.h>

#include "values.pb.h"

#define max(a,b) \
  ({ __typeof__ (a) _a = (a); \
    __typeof__ (b) _b = (b); \
    _a > _b ? _a : _b; })
#define min(a,b) \
  ({ __typeof__ (a) _a = (a); \
    __typeof__ (b) _b = (b); \
    _a < _b ? _a : _b; })

// will be filled out before operation begins
std::vector<uint8_t> key_prefix;

fuse_ino_t generate_inode()
{
  // TODO everything that uses this will need to check that
  // it isn't trampling an existing inode.
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

std::vector<uint8_t> pack_inode_key(fuse_ino_t ino, size_t extra_size)
{
  std::vector<uint8_t> key = key_prefix;
  key.push_back(INODE_PREFIX);
  fuse_ino_t tmp = htobe64(ino);
  uint8_t *tmpp = reinterpret_cast<uint8_t *>(&tmp);
  key.insert(key.end(), tmpp, tmpp + sizeof(fuse_ino_t));
  return key;
}

std::vector<uint8_t> pack_fileblock_key(fuse_ino_t ino, uint64_t block, size_t extra_size)
{
  auto key = pack_inode_key(ino, sizeof(uint64_t) + extra_size);
  key.push_back(DATA_PREFIX);
  
  block = htobe64(block);
  uint8_t *tmpp = reinterpret_cast<uint8_t *>(&block);
  key.insert(key.end(), tmpp, tmpp + sizeof(uint64_t));
  return key;
}

std::vector<uint8_t> pack_dentry_key(fuse_ino_t ino, std::string name)
{
  auto key = pack_inode_key(ino, name.size());
  key.push_back(DENTRY_PREFIX);

  key.insert(key.end(), name.begin(), name.end());
  return key;
}

void print_key(std::vector<uint8_t> v)
{
  printf("%zu ", v.size());
  for (std::vector<uint8_t>::const_iterator i = v.begin(); i != v.end(); ++i)
    printf("%02x", *i);
  printf("\n");
}

void pack_inode_record_into_stat(INodeRecord *inode, struct stat *attr)
{
  if(inode == NULL) {
    printf("got bad inode to repack into attr\n");
  }

  bzero(attr, sizeof(struct stat));
  
  attr->st_ino = inode->inode();
  attr->st_dev = 0;
  attr->st_mode = inode->mode() | inode->type();
  attr->st_nlink = inode->nlinks();
  if(inode->has_uid())
    attr->st_uid = inode->uid();
  else
    attr->st_uid = 0;

  if(inode->has_gid())
    attr->st_gid = inode->gid();
  else
    attr->st_gid = 0;

  if(inode->has_size())
    attr->st_size = inode->size();
  else
    attr->st_size = 0;

  if(inode->has_atime()) {
    attr->st_atim.tv_sec = inode->atime().sec();
    attr->st_atim.tv_nsec = inode->atime().nsec();
  }

  if(inode->has_mtime()) {
    attr->st_mtim.tv_sec = inode->mtime().sec();
    attr->st_mtim.tv_nsec = inode->mtime().nsec();
  }

  if(inode->has_ctime()) {
    attr->st_ctim.tv_sec = inode->ctime().sec();
    attr->st_ctim.tv_nsec = inode->ctime().nsec();
  }

  attr->st_blksize = BLOCKSIZE;
  attr->st_blocks = attr->st_size / BLOCKSIZE + 1;

  /*
  printf("stat struct\n");
  printf("  dev: %li\n", attr->st_dev);
  printf("  ino: %li\n", attr->st_ino);
  printf(" mode: %o\n", attr->st_mode);
  printf("nlink: %li\n", attr->st_nlink);
  printf("  uid: %i\n", attr->st_uid);
  printf("  gid: %i\n", attr->st_gid);
  */
}

void unpack_stat_from_dbvalue(const uint8_t *val, int vallen, struct stat *attr)
{
  INodeRecord inode;
  inode.ParseFromArray(val, vallen);
  pack_inode_record_into_stat(&inode, attr);
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
