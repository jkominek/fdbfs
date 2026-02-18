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
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/wait.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr long FUSE_SUPER_MAGIC = 0x65735546; // "eUsF"

enum class DatasetProfile { Empty, Seeded };

struct SeedConfig {
  int depth = 2;
  int entries_per_dir = 6;
  int dirs_per_dir = 2;
  size_t max_file_size = 5000;
  uint64_t rng_seed = 0x6a09e667f3bcc909ULL;
};

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
  struct statfs s{};
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
                    const fs::path &stdout_path, const fs::path &stderr_path) {
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

std::string profile_name(DatasetProfile p) {
  switch (p) {
  case DatasetProfile::Empty:
    return "empty";
  case DatasetProfile::Seeded:
    return "seeded";
  }
  return "unknown";
}

std::vector<DatasetProfile> dataset_profiles_from_env() {
  const char *raw = std::getenv("FDBFS_TEST_MATRIX");
  if (!raw || raw[0] == '\0') {
    return {DatasetProfile::Empty, DatasetProfile::Seeded};
  }

  std::vector<DatasetProfile> profiles;
  std::stringstream ss(raw);
  std::string token;
  while (std::getline(ss, token, ',')) {
    for (char &c : token) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (token == "empty") {
      profiles.push_back(DatasetProfile::Empty);
    } else if (token == "seeded") {
      profiles.push_back(DatasetProfile::Seeded);
    } else if (!token.empty()) {
      throw std::runtime_error("unknown FDBFS_TEST_MATRIX token: " + token);
    }
  }
  if (profiles.empty()) {
    throw std::runtime_error("FDBFS_TEST_MATRIX selected no profiles");
  }
  return profiles;
}

void require_posix_success(int rc, const std::string &what) {
  if (rc == 0) {
    return;
  }
  throw std::runtime_error(what + " failed: " + std::strerror(errno));
}

void write_file_contents(const fs::path &path,
                         const std::vector<uint8_t> &data) {
  int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0) {
    throw std::runtime_error("open(" + path.string() +
                             ") failed: " + std::strerror(errno));
  }

  size_t off = 0;
  while (off < data.size()) {
    const ssize_t n = ::write(fd, data.data() + off, data.size() - off);
    if (n < 0) {
      const int saved = errno;
      (void)::close(fd);
      throw std::runtime_error("write(" + path.string() +
                               ") failed: " + std::strerror(saved));
    }
    off += static_cast<size_t>(n);
  }
  require_posix_success(::close(fd), "close(" + path.string() + ")");
}

std::vector<uint8_t> generate_bytes(std::mt19937_64 &rng, size_t size) {
  std::vector<uint8_t> data(size, 0);
  std::uniform_int_distribution<int> mode_dist(0, 2);
  const int mode = mode_dist(rng);

  if (mode == 0) {
    std::fill(data.begin(), data.end(), static_cast<uint8_t>('A'));
  } else if (mode == 1) {
    std::fill(data.begin(), data.end(), static_cast<uint8_t>(0));
  } else {
    std::uniform_int_distribution<int> byte_dist(0, 255);
    for (auto &b : data) {
      b = static_cast<uint8_t>(byte_dist(rng));
    }
  }

  return data;
}

void maybe_set_random_xattrs(const fs::path &path, std::mt19937_64 &rng,
                             uint64_t &name_counter) {
  std::uniform_int_distribution<int> do_xattr(0, 1);
  if (do_xattr(rng) == 0) {
    return;
  }

  std::uniform_int_distribution<int> count_dist(1, 3);
  std::uniform_int_distribution<int> len_dist(1, 64);
  std::uniform_int_distribution<int> byte_dist(0, 255);
  const int count = count_dist(rng);
  for (int i = 0; i < count; i++) {
    const std::string name =
        "user.seed." + std::to_string(name_counter++) + "." + std::to_string(i);
    const size_t value_len = static_cast<size_t>(len_dist(rng));
    std::vector<uint8_t> value(value_len, 0);
    for (auto &b : value) {
      b = static_cast<uint8_t>(byte_dist(rng));
    }
    if (::setxattr(path.c_str(), name.c_str(), value.data(), value.size(), 0) !=
        0) {
      throw std::runtime_error("setxattr(" + path.string() + "," + name +
                               ") failed: " + std::strerror(errno));
    }
  }
}

void populate_seeded_dataset(const fs::path &mnt, const SeedConfig &cfg) {
  std::mt19937_64 rng(cfg.rng_seed);
  uint64_t name_counter = 0;

  const fs::path root = mnt / "__seed_data";
  require_posix_success(::mkdir(root.c_str(), 0755),
                        "mkdir(" + root.string() + ")");
  maybe_set_random_xattrs(root, rng, name_counter);

  std::vector<fs::path> current_level = {root};
  std::uniform_int_distribution<int> kind_dist(0, 2); // file/symlink/fifo
  std::uniform_int_distribution<size_t> file_size_dist(0, cfg.max_file_size);

  for (int depth = 0; depth <= cfg.depth; depth++) {
    std::vector<fs::path> next_level;
    for (const auto &dir : current_level) {
      std::vector<int> slots(cfg.entries_per_dir);
      for (int i = 0; i < cfg.entries_per_dir; i++) {
        slots[i] = i;
      }
      std::shuffle(slots.begin(), slots.end(), rng);

      const int dir_slots =
          (depth < cfg.depth) ? std::min(cfg.dirs_per_dir, cfg.entries_per_dir)
                              : 0;

      for (int idx = 0; idx < cfg.entries_per_dir; idx++) {
        const int slot_id = slots[idx];
        if (idx < dir_slots) {
          const fs::path child = dir / ("d_" + std::to_string(depth) + "_" +
                                        std::to_string(slot_id));
          require_posix_success(::mkdir(child.c_str(), 0755),
                                "mkdir(" + child.string() + ")");
          maybe_set_random_xattrs(child, rng, name_counter);
          next_level.push_back(child);
          continue;
        }

        const int kind = kind_dist(rng);
        if (kind == 0) {
          const fs::path p = dir / ("f_" + std::to_string(depth) + "_" +
                                    std::to_string(slot_id));
          const size_t size = file_size_dist(rng);
          write_file_contents(p, generate_bytes(rng, size));
          maybe_set_random_xattrs(p, rng, name_counter);
        } else if (kind == 1) {
          const fs::path p = dir / ("l_" + std::to_string(depth) + "_" +
                                    std::to_string(slot_id));
          const std::string target =
              "../missing-target-" + std::to_string(slot_id);
          require_posix_success(::symlink(target.c_str(), p.c_str()),
                                "symlink(" + p.string() + ")");
        } else {
          const fs::path p = dir / ("p_" + std::to_string(depth) + "_" +
                                    std::to_string(slot_id));
          require_posix_success(::mkfifo(p.c_str(), 0644),
                                "mkfifo(" + p.string() + ")");
        }
      }
    }
    current_level = std::move(next_level);
  }
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
  static const std::vector<DatasetProfile> profiles =
      dataset_profiles_from_env();
  const SeedConfig seed_cfg{};
  static std::atomic<unsigned int> next_empty_idx = 0;
  static std::atomic<unsigned int> next_seeded_idx = 0;

  for (const DatasetProfile profile : profiles) {
    FdbfsEnv env("case-" + profile_name(profile));
    const std::string key_prefix = [&]() {
      if (profile == DatasetProfile::Empty) {
        return std::string("empty") +
               std::to_string(next_empty_idx.fetch_add(1));
      }
      return std::string("seeded") +
             std::to_string(next_seeded_idx.fetch_add(1));
    }();

    INFO("filesystem dataset profile: " << profile_name(profile));
    INFO("filesystem case dir: " << env.root.filename().string());
    INFO("filesystem artifacts dir: " << env.artifacts.string());
    INFO("filesystem key prefix: " << key_prefix);
    env.append_op("dataset profile: " + profile_name(profile));
    env.append_op("using key_prefix=" + key_prefix);
    reset_database_with_gen_py(source_dir, env.artifacts, key_prefix);
    env.start_fdbfs(fs_exe, key_prefix);

    if (profile == DatasetProfile::Seeded) {
      env.append_op("seed begin");
      env.append_op(
          "seed cfg: depth=" + std::to_string(seed_cfg.depth) +
          " entries_per_dir=" + std::to_string(seed_cfg.entries_per_dir) +
          " dirs_per_dir=" + std::to_string(seed_cfg.dirs_per_dir) +
          " max_file_size=" + std::to_string(seed_cfg.max_file_size) +
          " rng_seed=" + std::to_string(seed_cfg.rng_seed));
      populate_seeded_dataset(env.mnt, seed_cfg);
      env.append_op("seed end");
    }

    try {
      fn(env);
      env.keep = false;
    } catch (...) {
      env.capture_state("assertion failure or exception");
      throw;
    }
  }
}

void scenario(const std::function<void(FdbfsEnv &)> &fn) {
  static const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  static const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");
  scenario(fs_exe, source_dir, fn);
}
