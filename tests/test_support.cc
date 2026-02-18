#include "test_support.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cctype>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <fcntl.h>
#include <sys/statfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr long FUSE_SUPER_MAGIC = 0x65735546; // "eUsF"

std::string sanitize(std::string s) {
  for (char &c : s) {
    if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '-' ||
          c == '.')) {
      c = '_';
    }
  }
  return s;
}

fs::path mkdtemp_dir(fs::path base, std::string_view prefix) {
  fs::create_directories(base);
  std::string tmpl = (base / (std::string(prefix) + "-XXXXXX")).string();
  std::vector<char> buf(tmpl.begin(), tmpl.end());
  buf.push_back('\0');
  char *out = ::mkdtemp(buf.data());
  if (!out) {
    throw std::runtime_error(std::string("mkdtemp failed: ") +
                             std::strerror(errno));
  }
  return fs::path(out);
}

void write_text(const fs::path &p, std::string_view s) {
  std::ofstream f(p, std::ios::binary);
  f.write(s.data(), static_cast<std::streamsize>(s.size()));
}

void best_effort_copy_file(const fs::path &from, const fs::path &to) noexcept {
  try {
    std::error_code ec;
    fs::copy_file(from, to, fs::copy_options::overwrite_existing, ec);
  } catch (...) {
  }
}

void dump_mountinfo(const fs::path &out) noexcept {
  try {
    std::ifstream in("/proc/self/mountinfo");
    std::ofstream o(out, std::ios::binary);
    o << in.rdbuf();
  } catch (...) {
  }
}

void dump_tree(const fs::path &root, const fs::path &out) noexcept {
  try {
    std::ofstream o(out);
    std::error_code ec;
    for (fs::recursive_directory_iterator it(
             root, fs::directory_options::skip_permission_denied, ec);
         !ec && it != fs::recursive_directory_iterator(); it.increment(ec)) {
      o << it->path().string() << "\n";
    }
  } catch (...) {
  }
}

bool is_fuse_mounted(const fs::path &mnt) {
  struct statfs s {};
  if (::statfs(mnt.c_str(), &s) != 0) {
    return false;
  }
  return s.f_type == FUSE_SUPER_MAGIC;
}

bool wait_for_mount(const fs::path &mnt, std::chrono::milliseconds timeout) {
  using namespace std::chrono;
  auto deadline = steady_clock::now() + timeout;
  while (steady_clock::now() < deadline) {
    if (is_fuse_mounted(mnt)) {
      return true;
    }
    ::usleep(10 * 1000);
  }
  return is_fuse_mounted(mnt);
}

int run_cmd_capture(const std::vector<std::string> &argv,
                    const fs::path &stdout_path,
                    const fs::path &stderr_path) {
  pid_t pid = ::fork();
  if (pid < 0) {
    return -1;
  }
  if (pid == 0) {
    int out = ::open(stdout_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    int err = ::open(stderr_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (out >= 0) {
      ::dup2(out, STDOUT_FILENO);
    }
    if (err >= 0) {
      ::dup2(err, STDERR_FILENO);
    }

    std::vector<char *> cargv;
    cargv.reserve(argv.size() + 1);
    for (auto &s : argv) {
      cargv.push_back(const_cast<char *>(s.c_str()));
    }
    cargv.push_back(nullptr);
    ::execvp(cargv[0], cargv.data());
    _exit(127);
  }
  int status = 0;
  (void)::waitpid(pid, &status, 0);
  return status;
}

bool exited_ok(int status) {
  return status >= 0 && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

std::string shell_quote(std::string_view s) {
  std::string out;
  out.reserve(s.size() + 2);
  out.push_back('\'');
  for (char c : s) {
    if (c == '\'') {
      out += "'\"'\"'";
    } else {
      out.push_back(c);
    }
  }
  out.push_back('\'');
  return out;
}

void reset_database_with_gen_py(const fs::path &source_dir,
                                const fs::path &artifacts_dir,
                                std::string_view key_prefix) {
  const fs::path out = artifacts_dir / "regen.stdout";
  const fs::path err = artifacts_dir / "regen.stderr";
  const std::string cmd = "cd " + shell_quote(source_dir.string()) +
                          " && ./gen.py " +
                          shell_quote(std::string(key_prefix)) + " | fdbcli";
  const int status = run_cmd_capture({"sh", "-c", cmd}, out, err);
  if (!exited_ok(status)) {
    throw std::runtime_error("database initialization failed; see " +
                             out.string() + " and " + err.string());
  }
}

} // namespace

fs::path required_env_path(const char *name) {
  const char *v = std::getenv(name);
  if (!v || v[0] == '\0') {
    throw std::runtime_error(std::string("missing required env var: ") + name);
  }
  return fs::path(v);
}

FdbfsEnv::FdbfsEnv(std::string test_name) {
  auto base = fs::temp_directory_path() / "fdbfs-tests";
  root = mkdtemp_dir(base, sanitize(test_name));

  artifacts = root / "artifacts";
  mnt = root / "mnt";
  fs::create_directories(artifacts);
  fs::create_directories(mnt);

  fdbfs_stderr = artifacts / "fdbfs.stderr";
  fdbfs_stdout = artifacts / "fdbfs.stdout";
  ops_log = artifacts / "ops.log";

  append_op("env created: root=" + root.string());
}

FdbfsEnv::~FdbfsEnv() noexcept {
  stop_fdbfs_best_effort();
  if (!keep) {
    std::error_code ec;
    fs::remove_all(root, ec);
  }
}

fs::path FdbfsEnv::p(std::string_view rel) const { return mnt / fs::path(rel); }

void FdbfsEnv::append_op(const std::string &s) noexcept {
  try {
    std::ofstream o(ops_log, std::ios::app);
    o << s << "\n";
  } catch (...) {
  }
}

void FdbfsEnv::start_fdbfs(const fs::path &fs_exe,
                           const std::string &key_prefix) {
  append_op("start_fdbfs: FDBFS_TRACE_ERRORS=1 " + fs_exe.string() +
            " --buggify -k " + key_prefix + " " + mnt.string());

  pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error(std::string("fork failed: ") +
                             std::strerror(errno));
  }

  if (pid == 0) {
    // New process group so we can kill everything if needed.
    (void)::setpgid(0, 0);
    (void)::setenv("FDBFS_TRACE_ERRORS", "1", 1);

    int out = ::open(fdbfs_stdout.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    int err = ::open(fdbfs_stderr.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (out >= 0) {
      ::dup2(out, STDOUT_FILENO);
    }
    if (err >= 0) {
      ::dup2(err, STDERR_FILENO);
    }

    ::execl(fs_exe.c_str(), fs_exe.c_str(), "--buggify", "-k",
            key_prefix.c_str(), mnt.c_str(), (char *)nullptr);
    _exit(127);
  }

  fuse_pid = pid;

  if (!wait_for_mount(mnt, std::chrono::milliseconds(2000))) {
    capture_state("mount did not become ready within timeout");
    stop_fdbfs_best_effort();
    throw std::runtime_error("FUSE mount did not become ready");
  }
}

void FdbfsEnv::stop_fdbfs_best_effort() noexcept {
  if (fuse_pid <= 0) {
    return;
  }

  append_op("stop_fdbfs_best_effort: pid=" + std::to_string(fuse_pid));

  // Try clean unmount first.
  (void)run_cmd_capture({"fusermount3", "-u", mnt.string()},
                        artifacts / "fusermount3.stdout",
                        artifacts / "fusermount3.stderr");

  // If still mounted, kill the process group and try unmount again.
  if (is_fuse_mounted(mnt)) {
    ::kill(-fuse_pid, SIGTERM);
    ::usleep(200 * 1000);
    if (is_fuse_mounted(mnt)) {
      ::kill(-fuse_pid, SIGKILL);
      ::usleep(200 * 1000);
      (void)run_cmd_capture({"fusermount3", "-u", mnt.string()},
                            artifacts / "fusermount3_2.stdout",
                            artifacts / "fusermount3_2.stderr");
    }
  }

  int status = 0;
  (void)::waitpid(fuse_pid, &status, WNOHANG);
  fuse_pid = -1;
}

void FdbfsEnv::capture_state(std::string_view why) noexcept {
  append_op(std::string("CAPTURE: ") + std::string(why));
  write_text(artifacts / "reason.txt", std::string(why) + "\n");

  dump_mountinfo(artifacts / "mountinfo.txt");
  dump_tree(root, artifacts / "host_tree.txt");

  // Try listing the FUSE tree; best-effort.
  try {
    dump_tree(mnt, artifacts / "fuse_tree.txt");
  } catch (...) {
  }

  best_effort_copy_file(fdbfs_stderr, artifacts / "fdbfs.stderr.copy");
  best_effort_copy_file(fdbfs_stdout, artifacts / "fdbfs.stdout.copy");
}

void scenario(const fs::path &fs_exe, const fs::path &source_dir,
              const std::function<void(FdbfsEnv &)> &fn) {
  FdbfsEnv env("case");
  static std::atomic<unsigned int> next_test_idx = 0;
  const std::string key_prefix =
      "t" + std::to_string(next_test_idx.fetch_add(1));
  INFO("filesystem case dir: " << env.root.filename().string());
  INFO("filesystem artifacts dir: " << env.artifacts.string());
  INFO("filesystem key prefix: " << key_prefix);
  env.append_op("using key_prefix=" + key_prefix);
  reset_database_with_gen_py(source_dir, env.artifacts, key_prefix);
  env.start_fdbfs(fs_exe, key_prefix);

  try {
    fn(env);
    env.keep = false;
  } catch (...) {
    env.capture_state("assertion failure or exception");
    throw;
  }
}

void scenario(const std::function<void(FdbfsEnv &)> &fn) {
  static const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  static const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");
  scenario(fs_exe, source_dir, fn);
}
