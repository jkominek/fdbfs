#include "util_fuse.h"

#include <assert.h>
#include <fcntl.h>

#include "filehandle.h"
#include "generic/util.h"

namespace {

std::shared_ptr<struct fdbfs_filehandle> **
extract_fdbfs_filehandle_slot(struct fuse_file_info *fi) {
  static_assert(sizeof(fi->fh) >=
                    sizeof(std::shared_ptr<struct fdbfs_filehandle> *),
                "FUSE File handle can't hold a pointer to our structure");
  return reinterpret_cast<std::shared_ptr<struct fdbfs_filehandle> **>(
      &(fi->fh));
}

} // namespace

std::shared_ptr<struct fdbfs_filehandle>
extract_fdbfs_filehandle(struct fuse_file_info *fi) {
  if (fi == nullptr) {
    return {};
  }
  auto **slot = extract_fdbfs_filehandle_slot(fi);
  if (slot == nullptr || *slot == nullptr) {
    return {};
  }
  return **slot;
}

void free_fdbfs_filehandle_slot(struct fuse_file_info *fi) {
  if (fi == nullptr) {
    return;
  }
  auto **slot = extract_fdbfs_filehandle_slot(fi);
  if (slot == nullptr || *slot == nullptr) {
    return;
  }
  delete *slot;
  *slot = nullptr;
}

int reply_open_with_handle(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info *fi) {
  bool atime = true;
#ifdef O_NOATIME
  atime = ((fi->flags & O_NOATIME) == 0);
#endif
  auto slot = new std::shared_ptr<struct fdbfs_filehandle>(
      std::make_shared<struct fdbfs_filehandle>(ino, atime));
  *(extract_fdbfs_filehandle_slot(fi)) = slot;

  auto generation = increment_lookup_count(ino);
  // open should only arrive for an inode that was already looked up.
  assert(!generation.has_value());

  if (fuse_reply_open(req, fi) < 0) {
    // release won't be called if open failed.
    free_fdbfs_filehandle_slot(fi);

    auto clear_generation = decrement_lookup_count(ino, 1);
    // paired decrement should never drop to zero under the same invariant.
    assert(!clear_generation.has_value());
    return -1;
  }
  return 0;
}
