#include "directoryhandle.h"

#include <utility>

DirectoryHandle::DirectoryHandle(fuse_ino_t ino) : ino_(ino) {}

void DirectoryHandle::close() { closed_ = true; }

bool DirectoryHandle::is_closed() const { return closed_; }

fuse_ino_t DirectoryHandle::inode() const { return ino_; }

uint64_t DirectoryHandle::filename_to_cookie(std::string_view name) {
  std::scoped_lock<std::mutex> guard(cookie_mutex_);

  if (auto it = filename_cookies_.find(std::string(name));
      it != filename_cookies_.end()) {
    return it->second;
  }

  const uint64_t cookie = static_cast<uint64_t>(cookie_filenames_.size()) + 1;
  cookie_filenames_.emplace_back(name);
  filename_cookies_.emplace(cookie_filenames_.back(), cookie);
  return cookie;
}

std::optional<std::string_view>
DirectoryHandle::cookie_to_filename(uint64_t cookie) const {
  std::scoped_lock<std::mutex> guard(cookie_mutex_);
  if (cookie == 0) {
    return std::nullopt;
  }

  const size_t index = static_cast<size_t>(cookie - 1);
  if (index >= cookie_filenames_.size()) {
    return std::nullopt;
  }
  return std::string_view(cookie_filenames_[index]);
}
