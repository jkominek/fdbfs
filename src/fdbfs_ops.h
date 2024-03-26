#ifndef __FDBFS_OPS_H__
#define __FDBFS_OPS_H__

#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>

extern "C" void fdbfs_init(void *userdata, struct fuse_conn_info *conn);
extern "C" void fdbfs_destroy(void *userdata);
extern "C" void fdbfs_lookup(fuse_req_t req, fuse_ino_t parent,
                             const char *name);
extern "C" void fdbfs_getattr(fuse_req_t req, fuse_ino_t ino,
                              struct fuse_file_info *fi);
extern "C" void fdbfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                              off_t off, struct fuse_file_info *fi);
extern "C" void fdbfs_open(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info *fi);
extern "C" void fdbfs_release(fuse_req_t req, fuse_ino_t ino,
                              struct fuse_file_info *fi);
extern "C" void fdbfs_read(fuse_req_t req, fuse_ino_t ino, size_t size,
                           off_t off, struct fuse_file_info *fi);
extern "C" void fdbfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
                            mode_t mode, dev_t rdev);
extern "C" void fdbfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                            mode_t mode);
extern "C" void fdbfs_unlink(fuse_req_t req, fuse_ino_t parent,
                             const char *name);
extern "C" void fdbfs_rmdir(fuse_req_t req, fuse_ino_t parent,
                            const char *name);
extern "C" void fdbfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                           const char *newname);
extern "C" void fdbfs_readlink(fuse_req_t req, fuse_ino_t ino);
extern "C" void fdbfs_symlink(fuse_req_t req, const char *link,
                              fuse_ino_t parent, const char *name);
extern "C" void fdbfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                              int to_set, struct fuse_file_info *fi);
extern "C" void fdbfs_rename(fuse_req_t req, fuse_ino_t parent,
                             const char *name, fuse_ino_t newparent,
                             const char *newname, unsigned int flags);
extern "C" void fdbfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
                            size_t size, off_t off, struct fuse_file_info *fi);
extern "C" void fdbfs_forget(fuse_req_t req, fuse_ino_t ino, uint64_t ncount);
extern "C" void fdbfs_forget_multi(fuse_req_t req, size_t count,
                                   struct fuse_forget_data *forgets);
extern "C" void fdbfs_statfs(fuse_req_t req, fuse_ino_t ino);
extern "C" void fdbfs_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                               size_t size);
extern "C" void fdbfs_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                               const char *value, size_t size, int flags);
extern "C" void fdbfs_removexattr(fuse_req_t req, fuse_ino_t ino,
                                  const char *name);
extern "C" void fdbfs_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size);
extern "C" void fdbfs_flush(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi);

#endif
