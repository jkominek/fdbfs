#include "test_support.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdlib>
#include <cstring>
#include <expected>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "generic/fdb_service.h"
#include "generic/fdbfs_runtime.h"
#include "generic/garbage_collector.h"
#include "generic/liveness.h"
#include "generic/mknod.hpp"
#include "generic/util_locks.h"
#include "generic/write.hpp"
#include "test_inflight_action.h"

namespace fs = std::filesystem;

uint8_t BLOCKBITS = 13;
uint32_t BLOCKSIZE = 1u << 13;

namespace {

constexpr auto kCommandTimeout = std::chrono::seconds(10);
constexpr auto kProcessReadyTimeout = std::chrono::seconds(10);
constexpr fdbfs_ino_t kRootIno = 1;

fs::path required_env_path(const char *name) {
  const char *v = std::getenv(name);
  if ((v == nullptr) || (v[0] == '\0')) {
    throw std::runtime_error(std::string("missing required env var: ") + name);
  }
  return fs::path(v);
}

std::string optional_env_string(const char *name) {
  const char *v = std::getenv(name);
  if ((v == nullptr) || (v[0] == '\0')) {
    return {};
  }
  return std::string(v);
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

void reset_database_with_mkfs(std::string_view prefix) {
  const auto mkfs_exe = required_env_path("FDBFS_MKFS_EXE");
  const std::string cmd =
      mkfs_exe.string() + " --force " + std::string(prefix);
  REQUIRE(std::system(cmd.c_str()) == 0);
}

std::unique_ptr<TestRequest> make_test_request() {
  return std::make_unique<TestRequest>();
}

std::future<TestResult> take_future(TestRequest *req) {
  return req->promise.get_future();
}

TestResult wait_test_result(std::future<TestResult> &future) {
  REQUIRE(future.wait_for(kCommandTimeout) == std::future_status::ready);
  return future.get();
}

std::expected<INodeRecord, int> wait_getinode(std::future<TestResult> &future) {
  TestResult result = wait_test_result(future);
  if (!result.has_value()) {
    return std::unexpected(result.error());
  }
  REQUIRE(std::holds_alternative<TestReplyINode>(*result));
  return std::get<TestReplyINode>(*result).inode;
}

std::string unique_prefix() {
  const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  return "nbdkit-" + std::to_string(getpid()) + "-" + std::to_string(now);
}

void initialize_generic_state(std::string_view prefix) {
  key_prefix.clear();
  key_prefix.insert(key_prefix.end(), prefix.begin(), prefix.end());
  shut_it_down_forever = false;
  inode_key_length = pack_inode_key(0).size();
  fileblock_prefix_length = inode_key_length;
  fileblock_key_length = pack_fileblock_key(0, 0).size();
  dirent_prefix_length = pack_dentry_key(0, "").size();
  BLOCKBITS = 13;
  BLOCKSIZE = 1u << BLOCKBITS;
  lookup_counts.reserve(128);
}

CommandResult run_cmd_capture(const std::vector<std::string> &argv) {
  int stdout_pipe[2];
  int stderr_pipe[2];
  REQUIRE(::pipe(stdout_pipe) == 0);
  REQUIRE(::pipe(stderr_pipe) == 0);

  pid_t pid = ::fork();
  REQUIRE(pid >= 0);

  if (pid == 0) {
    ::close(stdout_pipe[0]);
    ::close(stderr_pipe[0]);
    ::dup2(stdout_pipe[1], STDOUT_FILENO);
    ::dup2(stderr_pipe[1], STDERR_FILENO);
    ::close(stdout_pipe[1]);
    ::close(stderr_pipe[1]);

    std::vector<char *> cargv;
    cargv.reserve(argv.size() + 1);
    for (const auto &arg : argv) {
      cargv.push_back(const_cast<char *>(arg.c_str()));
    }
    cargv.push_back(nullptr);
    ::execvp(cargv[0], cargv.data());
    _exit(127);
  }

  ::close(stdout_pipe[1]);
  ::close(stderr_pipe[1]);

  auto read_all = [](int fd) {
    std::string out;
    char buf[4096];
    for (;;) {
      const ssize_t rc = ::read(fd, buf, sizeof(buf));
      if (rc > 0) {
        out.append(buf, static_cast<size_t>(rc));
        continue;
      }
      if (rc == 0) {
        break;
      }
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error(std::string("read failed: ") +
                               std::strerror(errno));
    }
    return out;
  };

  CommandResult result;
  result.stdout_text = read_all(stdout_pipe[0]);
  result.stderr_text = read_all(stderr_pipe[0]);
  ::close(stdout_pipe[0]);
  ::close(stderr_pipe[0]);
  REQUIRE(::waitpid(pid, &result.status, 0) == pid);
  return result;
}

uint16_t reserve_unused_tcp_port() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error(std::string("socket failed: ") +
                             std::strerror(errno));
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (::bind(fd, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) != 0) {
    const int err = errno;
    ::close(fd);
    throw std::runtime_error(std::string("bind failed: ") + std::strerror(err));
  }

  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
    const int err = errno;
    ::close(fd);
    throw std::runtime_error(std::string("getsockname failed: ") +
                             std::strerror(err));
  }
  const uint16_t port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

} // namespace

class NbdkitIntegratedServices::Impl {
public:
  Impl() : prefix(unique_prefix()) {
    initialize_generic_state(prefix);
    reset_database_with_mkfs(prefix);

    runtime.add_persistent<FdbService>(
        []() { return std::make_unique<FdbService>(false); });
    runtime.add_restartable<LivenessService>(
        []() { return std::make_unique<LivenessService>([]() {}); });
    runtime.add_restartable<GarbageCollectorService>(
        []() { return std::make_unique<GarbageCollectorService>(); });
    runtime.add_restartable<LockManagerService>(
        []() { return std::make_unique<LockManagerService>(); });
    g_fdbfs_runtime = &runtime;
    runtime.start_all();
  }

  ~Impl() {
    runtime.stop_restartable();
    if (!key_prefix.empty()) {
      const auto range = std::make_pair(key_prefix, prefix_range_end(key_prefix));
      (void)run_sync_transaction<int>([&](FDBTransaction *t) {
        fdbfs_transaction_clear_range(t, range);
        return 0;
      });
    }
  }

  std::string prefix;
  FdbfsRuntime runtime;
};

NbdkitIntegratedServices::NbdkitIntegratedServices() : impl_(new Impl()) {}

NbdkitIntegratedServices::~NbdkitIntegratedServices() { delete impl_; }

const std::string &NbdkitIntegratedServices::prefix() const {
  return impl_->prefix;
}

unique_transaction NbdkitIntegratedServices::make_transaction() const {
  return impl_->runtime.require<FdbService>().make_transaction();
}

INodeRecord NbdkitIntegratedServices::create_regular_file(std::string_view name) {
  auto req = make_test_request();
  auto future = take_future(req.get());
  auto *op = new Inflight_mknod<TestInflightAction, std::monostate>(
      req.get(), kRootIno, std::string(name), 0644, ft_regular, 0,
      make_transaction(), std::nullopt, std::monostate{});
  op->start();
  auto created = wait_getinode(future);
  REQUIRE(created.has_value());
  return *std::move(created);
}

INodeRecord NbdkitIntegratedServices::create_directory(std::string_view name) {
  auto req = make_test_request();
  auto future = take_future(req.get());
  auto *op = new Inflight_mknod<TestInflightAction, std::monostate>(
      req.get(), kRootIno, std::string(name), 0755, ft_directory, 0,
      make_transaction(), std::nullopt, std::monostate{});
  op->start();
  auto created = wait_getinode(future);
  REQUIRE(created.has_value());
  return *std::move(created);
}

void NbdkitIntegratedServices::write_file(fdbfs_ino_t ino,
                                          std::vector<uint8_t> bytes,
                                          off_t off) {
  auto req = make_test_request();
  auto future = take_future(req.get());
  auto *op = new Inflight_write<TestInflightAction>(
      req.get(), ino, WritePayloadBytes{.bytes = std::move(bytes)},
      WritePosOffset{.off = off}, make_transaction());
  op->start();
  TestResult result = wait_test_result(future);
  REQUIRE(result.has_value());
  REQUIRE(std::holds_alternative<TestReplyWrite>(*result));
}

NbdkitIntegratedServices &nbdkit_integrated_services() {
  static NbdkitIntegratedServices services;
  return services;
}

bool nbdkit_integrated_tools_available() {
  return !optional_env_string("FDBFS_NBDKIT_BIN").empty() &&
         !optional_env_string("FDBFS_NBDINFO_BIN").empty() &&
         !optional_env_string("FDBFS_NBDCOPY_BIN").empty() &&
         !optional_env_string("FDBFS_NBDKIT_PLUGIN").empty();
}

NbdkitProcess::NbdkitProcess(std::string prefix)
    : temp_dir_(mkdtemp_dir(fs::temp_directory_path(), "fdbfs-nbdkit")),
      stdout_path_(temp_dir_ / "stdout.log"),
      stderr_path_(temp_dir_ / "stderr.log"), port_(reserve_unused_tcp_port()) {
  const std::string nbdkit_bin = optional_env_string("FDBFS_NBDKIT_BIN");
  const std::string plugin_path = optional_env_string("FDBFS_NBDKIT_PLUGIN");
  if (nbdkit_bin.empty() || plugin_path.empty()) {
    throw std::runtime_error("missing nbdkit binary or plugin path");
  }

  pid_ = ::fork();
  REQUIRE(pid_ >= 0);

  if (pid_ == 0) {
    int out = ::open(stdout_path_.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    int err = ::open(stderr_path_.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (out >= 0) {
      ::dup2(out, STDOUT_FILENO);
      ::close(out);
    }
    if (err >= 0) {
      ::dup2(err, STDERR_FILENO);
      ::close(err);
    }

    const std::string key_prefix_arg = "key_prefix=" + prefix;
    const std::string port_arg = std::to_string(port_);
    ::execlp(nbdkit_bin.c_str(), nbdkit_bin.c_str(), "-f", "-p", port_arg.c_str(),
             "-i", "127.0.0.1", plugin_path.c_str(), key_prefix_arg.c_str(),
             nullptr);
    _exit(127);
  }

  const auto deadline = std::chrono::steady_clock::now() + kProcessReadyTimeout;
  while (std::chrono::steady_clock::now() < deadline) {
    CommandResult result =
        nbdinfo({"--list", root_uri()});
    if (WIFEXITED(result.status) && WEXITSTATUS(result.status) == 0) {
      return;
    }
    ::usleep(20 * 1000);
  }

  INFO("nbdkit stdout:\n" << std::ifstream(stdout_path_).rdbuf());
  INFO("nbdkit stderr:\n" << std::ifstream(stderr_path_).rdbuf());
  FAIL("nbdkit did not become ready");
}

NbdkitProcess::~NbdkitProcess() {
  if (pid_ > 0) {
    (void)::kill(pid_, SIGTERM);
    int status = 0;
    (void)::waitpid(pid_, &status, 0);
    pid_ = -1;
  }
}

std::string NbdkitProcess::root_uri() const {
  return "nbd://127.0.0.1:" + std::to_string(port_);
}

std::string NbdkitProcess::export_uri(std::string_view export_name) const {
  return "nbd://127.0.0.1:" + std::to_string(port_) + "/" +
         std::string(export_name);
}

CommandResult NbdkitProcess::nbdinfo(std::vector<std::string> args) const {
  std::vector<std::string> argv;
  argv.push_back(optional_env_string("FDBFS_NBDINFO_BIN"));
  argv.insert(argv.end(), args.begin(), args.end());
  return run_cmd_capture(argv);
}

CommandResult NbdkitProcess::nbdcopy(std::vector<std::string> args) const {
  std::vector<std::string> argv;
  argv.push_back(optional_env_string("FDBFS_NBDCOPY_BIN"));
  argv.insert(argv.end(), args.begin(), args.end());
  return run_cmd_capture(argv);
}
