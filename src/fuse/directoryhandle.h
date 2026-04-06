#ifndef __DIRECTORYHANDLE_H__
#define __DIRECTORYHANDLE_H__

#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>

#include <atomic>
#include <cstdint>
#include <ctime>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "generic/util.h"

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
  void read_complete();
  void reserve_cookies_through(uint64_t res);
  [[nodiscard]] uint64_t filename_to_cookie(std::string_view name);
  [[nodiscard]] std::optional<std::string_view>
  cookie_to_filename(uint64_t cookie) const;

private:
  // Requires lk to be held on state_mutex_ and releases it before returning.
  void launch_atime_update_locked(const struct timespec &target,
                                  const struct timespec &attempt_time,
                                  std::unique_lock<std::mutex> &lk);

  fuse_ino_t ino;
  bool closed = false;
  mutable std::mutex cookie_mutex;
  mutable std::mutex state_mutex;
  uint64_t reserved_cookies = 0;
  std::vector<std::string> cookie_filenames;
  std::unordered_map<std::string, uint64_t> filename_cookies;
  std::optional<struct timespec> latest_read;
  std::optional<struct timespec> first_read;
  std::optional<struct timespec> last_update_attempt;
  std::atomic<size_t> inflight_updates{0};
};

#endif
