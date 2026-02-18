#pragma once

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <functional>
#include <string>
#include <string_view>
#include <sys/types.h>

namespace fs = std::filesystem;

struct FdbfsEnv {
  fs::path root;      // host FS
  fs::path artifacts; // host FS
  fs::path mnt;       // becomes FUSE FS after mount

  fs::path fdbfs_stderr;
  fs::path fdbfs_stdout;
  fs::path ops_log;

  explicit FdbfsEnv(std::string test_name);
  ~FdbfsEnv() noexcept;

  fs::path p(std::string_view rel) const;
  void append_op(const std::string &s) noexcept;
  void start_fdbfs(const fs::path &fs_exe, const std::string &key_prefix);
  void stop_fdbfs_best_effort() noexcept;
  void capture_state(std::string_view why) noexcept;

  bool keep = true; // keep artifacts on failure by default
  pid_t fuse_pid = -1;
};

fs::path required_env_path(const char *name);

void scenario(const fs::path &fs_exe, const fs::path &source_dir,
              const std::function<void(FdbfsEnv &)> &fn);
void scenario(const std::function<void(FdbfsEnv &)> &fn);

inline std::string errno_with_message(int err) {
  const char *msg = std::strerror(err);
  std::string out = std::to_string(err);
  out += " (";
  out += (msg != nullptr) ? msg : "unknown";
  out += ")";
  return out;
}

#define FDBFS_REQUIRE_OK(expr)                                                 \
  do {                                                                         \
    const auto _fdbfs_rc = (expr);                                             \
    const int _fdbfs_errno = errno;                                            \
    INFO(#expr << " => rc=" << _fdbfs_rc                                       \
               << ", errno=" << errno_with_message(_fdbfs_errno));             \
    REQUIRE(_fdbfs_rc == 0);                                                   \
  } while (0)

#define FDBFS_REQUIRE_NONNEG(expr)                                             \
  do {                                                                         \
    const auto _fdbfs_rc = (expr);                                             \
    const int _fdbfs_errno = errno;                                            \
    INFO(#expr << " => rc=" << _fdbfs_rc                                       \
               << ", errno=" << errno_with_message(_fdbfs_errno));             \
    REQUIRE(_fdbfs_rc >= 0);                                                   \
  } while (0)

#define FDBFS_CHECK_ERRNO(expected)                                            \
  do {                                                                         \
    const int _fdbfs_errno = errno;                                            \
    INFO("errno=" << errno_with_message(_fdbfs_errno)                          \
                  << ", expected=" << errno_with_message(expected));           \
    CHECK(_fdbfs_errno == (expected));                                         \
  } while (0)
