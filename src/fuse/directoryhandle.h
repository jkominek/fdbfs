#ifndef __DIRECTORYHANDLE_H__
#define __DIRECTORYHANDLE_H__

#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>

class DirectoryHandle {
public:
  explicit DirectoryHandle(fuse_ino_t ino);
  ~DirectoryHandle() = default;

  DirectoryHandle(const DirectoryHandle &) = delete;
  DirectoryHandle &operator=(const DirectoryHandle &) = delete;
  DirectoryHandle(DirectoryHandle &&) = delete;
  DirectoryHandle &operator=(DirectoryHandle &&) = delete;

  void close();
  [[nodiscard]] bool is_closed() const;
  [[nodiscard]] fuse_ino_t inode() const;

private:
  fuse_ino_t ino_;
  bool closed_ = false;
};

#endif
