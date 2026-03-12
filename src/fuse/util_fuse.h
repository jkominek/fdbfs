#ifndef __FUSE_UTIL_FUSE_H__
#define __FUSE_UTIL_FUSE_H__

#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>

#include <memory>

struct fdbfs_filehandle;

[[nodiscard]] std::shared_ptr<struct fdbfs_filehandle>
extract_fdbfs_filehandle(struct fuse_file_info *fi);
void free_fdbfs_filehandle_slot(struct fuse_file_info *fi);
int reply_open_with_handle(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info *fi);

#endif
