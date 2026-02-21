#include <errno.h>

#include "fdbfs_ops.h"

/*************************************************************
 * fsync / fsyncdir
 *************************************************************
 * Stubbed for now.
 */

extern "C" void fdbfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                            struct fuse_file_info *fi) {
  (void)ino;
  (void)datasync;
  (void)fi;
  fuse_reply_err(req, ENOSYS);
}

extern "C" void fdbfs_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
                               struct fuse_file_info *fi) {
  (void)ino;
  (void)datasync;
  (void)fi;
  fuse_reply_err(req, ENOSYS);
}
