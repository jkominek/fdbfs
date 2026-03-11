#include <errno.h>

#include "fdbfs_ops.h"
#include "filehandle.h"
#include "fsync.h"
#include "util.h"

/*************************************************************
 * fsync / fsyncdir
 *************************************************************
 */

extern "C" void fdbfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                            struct fuse_file_info *fi) {
  (void)datasync;
  (void)fi;
  g_fsync_barrier_table.fsync_async(ino, req);
}

extern "C" void fdbfs_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
                               struct fuse_file_info *fi) {
  (void)datasync;
  (void)fi;
  g_fsync_barrier_table.fsync_async(ino, req);
}
