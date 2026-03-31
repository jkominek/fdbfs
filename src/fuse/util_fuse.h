#ifndef __FUSE_UTIL_FUSE_H__
#define __FUSE_UTIL_FUSE_H__

#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>

#include <cassert>
#include <memory>

#include "values.pb.h"

struct fdbfs_filehandle;

template <typename T>
[[nodiscard]] std::shared_ptr<T> **extract_fuse_handle_slot(
    struct fuse_file_info *fi) {
  static_assert(sizeof(fi->fh) >= sizeof(std::shared_ptr<T> *),
                "FUSE file handle can't hold a pointer to our structure");
  return reinterpret_cast<std::shared_ptr<T> **>(&(fi->fh));
}

template <typename T>
[[nodiscard]] std::shared_ptr<T> extract_fuse_handle(struct fuse_file_info *fi) {
  if (fi == nullptr) {
    return {};
  }
  auto **slot = extract_fuse_handle_slot<T>(fi);
  if ((slot == nullptr) || (*slot == nullptr)) {
    return {};
  }
  return **slot;
}

template <typename T>
void free_fuse_handle_slot(struct fuse_file_info *fi) {
  if (fi == nullptr) {
    return;
  }
  auto **slot = extract_fuse_handle_slot<T>(fi);
  if ((slot == nullptr) || (*slot == nullptr)) {
    return;
  }
  delete *slot;
  *slot = nullptr;
}

int reply_open_with_handle(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info *fi);
int reply_create_with_handle(fuse_req_t req, const INodeRecord &inode,
                             struct fuse_file_info *fi);

#endif
