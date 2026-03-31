#include "util_fuse.h"

#include <assert.h>
#include <fcntl.h>

#include "filehandle.h"
#include "generic/util.h"

int reply_open_with_handle(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info *fi) {
  auto slot = new std::shared_ptr<FileHandle>(std::make_shared<FileHandle>(ino));
#ifdef O_NOATIME
  (**slot).do_atime = ((fi->flags & O_NOATIME) == 0);
#else
  (**slot).do_atime = true;
#endif
  *(extract_fuse_handle_slot<FileHandle>(fi)) = slot;

  auto generation = increment_lookup_count(static_cast<fdbfs_ino_t>(ino));
  // open should only arrive for an inode that was already looked up.
  assert(!generation.has_value());

  if (fuse_reply_open(req, fi) < 0) {
    // release won't be called if open failed.
    free_fuse_handle_slot<FileHandle>(fi);

    auto clear_generation =
        decrement_lookup_count(static_cast<fdbfs_ino_t>(ino), 1);
    // paired decrement should never drop to zero under the same invariant.
    assert(!clear_generation.has_value());
    return -1;
  }
  return 0;
}

int reply_create_with_handle(fuse_req_t req, const INodeRecord &inode,
                             struct fuse_file_info *fi) {
  auto slot =
      new std::shared_ptr<FileHandle>(std::make_shared<FileHandle>(inode.inode()));
#ifdef O_NOATIME
  (**slot).do_atime = ((fi->flags & O_NOATIME) == 0);
#else
  (**slot).do_atime = true;
#endif
  *(extract_fuse_handle_slot<FileHandle>(fi)) = slot;

  struct fuse_entry_param e{};
  e.ino = static_cast<fuse_ino_t>(inode.inode());
  e.generation = 1;
  pack_inode_record_into_stat(inode, e.attr);
  e.attr_timeout = 0.01;
  e.entry_timeout = 0.01;

  if (fuse_reply_create(req, &e, fi) < 0) {
    free_fuse_handle_slot<FileHandle>(fi);

    auto clear_generation =
        decrement_lookup_count(static_cast<fdbfs_ino_t>(inode.inode()), 1);
    assert(clear_generation.has_value());
    best_effort_clear_inode_use_record(static_cast<fdbfs_ino_t>(inode.inode()),
                                       *clear_generation);
    return -1;
  }
  return 0;
}
