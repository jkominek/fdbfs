#pragma once

#include <catch2/catch_test_macros.hpp>

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <source_location>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace fs = std::filesystem;

inline std::string posix_errno_with_message(int err) {
  const char *msg = std::strerror(err);
  std::string out = std::to_string(err);
  out += " (";
  out += (msg != nullptr) ? msg : "unknown";
  out += ")";
  return out;
}

#define POSIX_REQUIRE_NONNEG(expr)                                             \
  do {                                                                         \
    const auto _posix_rc = (expr);                                             \
    const int _posix_errno = errno;                                            \
    INFO(#expr << " => rc=" << _posix_rc                                       \
               << ", errno=" << posix_errno_with_message(_posix_errno));       \
    REQUIRE(_posix_rc >= 0);                                                   \
  } while (0)

#define POSIX_REQUIRE_ZERO(expr)                                               \
  do {                                                                         \
    const auto _posix_rc = (expr);                                             \
    const int _posix_errno = errno;                                            \
    INFO(#expr << " => rc=" << _posix_rc                                       \
               << ", errno=" << posix_errno_with_message(_posix_errno));       \
    REQUIRE(_posix_rc == 0);                                                   \
  } while (0)

inline int compare_timespec(const struct timespec &a, const struct timespec &b) {
  if (a.tv_sec < b.tv_sec) {
    return -1;
  }
  if (a.tv_sec > b.tv_sec) {
    return 1;
  }
  if (a.tv_nsec < b.tv_nsec) {
    return -1;
  }
  if (a.tv_nsec > b.tv_nsec) {
    return 1;
  }
  return 0;
}

inline bool is_recent(const struct timespec &t, const struct timespec &now,
                      time_t max_age_sec) {
  if (compare_timespec(t, now) > 0) {
    return false;
  }
  return (now.tv_sec - t.tv_sec) <= max_age_sec;
}

inline mode_t current_umask() {
  const mode_t old_umask = ::umask(0);
  ::umask(old_umask);
  return old_umask;
}

inline void write_all_fd(int fd, const uint8_t *data, size_t size) {
  size_t written = 0;
  while (written < size) {
    const ssize_t n = ::write(fd, data + written, size - written);
    POSIX_REQUIRE_NONNEG(n);
    written += static_cast<size_t>(n);
  }
}

inline void pwrite_all_fd(int fd, const uint8_t *data, size_t size, off_t off) {
  size_t written = 0;
  while (written < size) {
    const ssize_t n = ::pwrite(fd, data + written, size - written,
                               off + static_cast<off_t>(written));
    POSIX_REQUIRE_NONNEG(n);
    written += static_cast<size_t>(n);
  }
}

inline std::vector<uint8_t> pread_exact_fd(int fd, size_t size, off_t off) {
  std::vector<uint8_t> out(size, 0);
  size_t consumed = 0;
  while (consumed < size) {
    const ssize_t n = ::pread(fd, out.data() + consumed, size - consumed,
                              off + static_cast<off_t>(consumed));
    POSIX_REQUIRE_NONNEG(n);
    if (n == 0) {
      break;
    }
    consumed += static_cast<size_t>(n);
  }
  out.resize(consumed);
  return out;
}

inline std::vector<uint8_t> read_file_all(const fs::path &p) {
  int fd = ::open(p.c_str(), O_RDONLY);
  POSIX_REQUIRE_NONNEG(fd);

  struct stat st {};
  POSIX_REQUIRE_ZERO(::fstat(fd, &st));
  REQUIRE(st.st_size >= 0);

  const size_t size = static_cast<size_t>(st.st_size);
  std::vector<uint8_t> out(size, 0);
  size_t consumed = 0;
  while (consumed < size) {
    const ssize_t n = ::read(fd, out.data() + consumed, size - consumed);
    POSIX_REQUIRE_NONNEG(n);
    if (n == 0) {
      break;
    }
    consumed += static_cast<size_t>(n);
  }
  POSIX_REQUIRE_ZERO(::close(fd));
  out.resize(consumed);
  return out;
}

inline std::vector<uint8_t> make_pattern(size_t size, uint64_t seed) {
  std::vector<uint8_t> out(size, 0);
  uint64_t x = seed;
  for (size_t i = 0; i < size; i++) {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    out[i] = static_cast<uint8_t>(x & 0xffu);
  }
  return out;
}

inline std::set<std::string> readdir_names(const fs::path &dir) {
  std::set<std::string> names;
  DIR *d = ::opendir(dir.c_str());
  POSIX_REQUIRE_NONNEG(d != nullptr ? 0 : -1);

  while (true) {
    errno = 0;
    struct dirent *ent = ::readdir(d);
    if (!ent) {
      INFO("readdir errno=" << posix_errno_with_message(errno));
      REQUIRE(errno == 0);
      break;
    }
    names.emplace(ent->d_name);
  }

  POSIX_REQUIRE_ZERO(::closedir(d));
  return names;
}

inline void require_contains(const std::set<std::string> &names,
                             std::string_view name,
                             std::source_location loc =
                                 std::source_location::current()) {
  INFO("require_contains caller=" << loc.file_name() << ":" << loc.line());
  INFO("needle=" << name << " set_size=" << names.size());
  REQUIRE(names.find(std::string(name)) != names.end());
}
