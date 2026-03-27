#ifndef __DIRECTORYHANDLE_H__
#define __DIRECTORYHANDLE_H__

#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

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
  [[nodiscard]] uint64_t filename_to_cookie(std::string_view name);
  [[nodiscard]] std::optional<std::string_view>
  cookie_to_filename(uint64_t cookie) const;

private:
  fuse_ino_t ino_;
  bool closed_ = false;
  mutable std::mutex cookie_mutex_;
  std::vector<std::string> cookie_filenames_;
  std::unordered_map<std::string, uint64_t> filename_cookies_;
};

#endif
