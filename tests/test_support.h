#pragma once

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
