#include "util_fuse.h"

#include <assert.h>
#include <fcntl.h>

#include "filehandle.h"
#include "generic/util.h"

int reply_open_with_handle(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info *fi) {
  bool atime = true;
#ifdef O_NOATIME
  atime = ((fi->flags & O_NOATIME) == 0);
#endif
  auto slot = new std::shared_ptr<struct fdbfs_filehandle>(
      std::make_shared<struct fdbfs_filehandle>(ino, atime));
  *(extract_fuse_handle_slot<struct fdbfs_filehandle>(fi)) = slot;

  auto generation = increment_lookup_count(static_cast<fdbfs_ino_t>(ino));
  // open should only arrive for an inode that was already looked up.
  assert(!generation.has_value());

  if (fuse_reply_open(req, fi) < 0) {
    // release won't be called if open failed.
    free_fuse_handle_slot<struct fdbfs_filehandle>(fi);

    auto clear_generation =
        decrement_lookup_count(static_cast<fdbfs_ino_t>(ino), 1);
    // paired decrement should never drop to zero under the same invariant.
    assert(!clear_generation.has_value());
    return -1;
  }
  return 0;
}
