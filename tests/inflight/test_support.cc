#include "test_support.h"

#include <cstdlib>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "generic/fdb_service.h"
#include "generic/fdbfs_runtime.h"
#include "generic/garbage_collector.h"
#include "generic/liveness.h"
#include "generic/util_locks.h"

uint8_t BLOCKBITS = 13;
uint32_t BLOCKSIZE = 1u << 13;

namespace {

std::filesystem::path required_env_path(const char *name) {
  const char *v = std::getenv(name);
  if ((v == nullptr) || (v[0] == '\0')) {
    throw std::runtime_error(std::string("missing required env var: ") + name);
  }
  return std::filesystem::path(v);
}

std::string shell_quote(std::string_view s) {
  std::string out;
  out.reserve(s.size() + 2);
  out.push_back('\'');
  for (char c : s) {
    if (c == '\'') {
      out += "'\\''";
    } else {
      out.push_back(c);
    }
  }
  out.push_back('\'');
  return out;
}

void reset_database_with_gen_py(const std::filesystem::path &source_dir,
                                std::string_view prefix) {
  const std::string cmd = "cd " + shell_quote(source_dir.string()) +
                          " && ./gen.py " + shell_quote(std::string(prefix)) +
                          " | fdbcli";
  const int status = std::system(cmd.c_str());
  if (status != 0) {
    throw std::runtime_error("database initialization via gen.py failed");
  }
}

} // namespace

class InflightTestServices::Impl {
public:
  Impl() {
    key_prefix = make_key_prefix();
    shut_it_down_forever = false;
    inode_key_length = pack_inode_key(0).size();
    fileblock_prefix_length = inode_key_length;
    fileblock_key_length = pack_fileblock_key(0, 0).size();
    dirent_prefix_length = pack_dentry_key(0, "").size();
    reset_database_with_gen_py(required_env_path("FDBFS_SOURCE_DIR"),
                               std::string_view(
                                   reinterpret_cast<const char *>(key_prefix.data()),
                                   key_prefix.size()));

    runtime.add_persistent<FdbService>(
        []() { return std::make_unique<FdbService>(false); });
    runtime.add_restartable<LivenessService>(
        []() { return std::make_unique<LivenessService>([]() {}); });
    runtime.add_restartable<GarbageCollectorService>(
        []() { return std::make_unique<GarbageCollectorService>(); });
    runtime.add_restartable<LockManagerService>(
        []() { return std::make_unique<LockManagerService>(); });
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

  [[nodiscard]] unique_transaction make_transaction() const {
    return runtime.require<FdbService>().make_transaction();
  }

private:
  static std::vector<uint8_t> make_key_prefix() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::string prefix =
        "inflight-" + std::to_string(getpid()) + "-" + std::to_string(now);
    return std::vector<uint8_t>(prefix.begin(), prefix.end());
  }

  FdbfsRuntime runtime;
};

InflightTestServices::InflightTestServices() : impl_(new Impl()) {}

InflightTestServices::~InflightTestServices() { delete impl_; }

unique_transaction InflightTestServices::make_transaction() const {
  return impl_->make_transaction();
}

InflightTestServices &inflight_test_services() {
  static InflightTestServices services;
  return services;
}

std::unique_ptr<TestRequest> make_test_request() {
  return std::make_unique<TestRequest>();
}

std::future<TestResult> take_future(TestRequest *req) {
  return req->promise.get_future();
}
