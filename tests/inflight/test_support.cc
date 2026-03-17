#include "test_support.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdlib>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "generic/fdb_service.h"
#include "generic/fdbfs_runtime.h"
#include "generic/forget.hpp"
#include "generic/garbage_collector.h"
#include "generic/getinode.hpp"
#include "generic/link.hpp"
#include "generic/liveness.h"
#include "generic/lookup.hpp"
#include "generic/mknod.hpp"
#include "generic/rename.hpp"
#include "generic/util_locks.h"

uint8_t BLOCKBITS = 13;
uint32_t BLOCKSIZE = 1u << 13;

namespace {

constexpr auto kInflightTestTimeout = std::chrono::seconds(5);

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

std::expected<TestReply, int>
wait_for_test_result(std::future<TestResult> &future) {
  if (future.wait_for(kInflightTestTimeout) != std::future_status::ready) {
    return std::unexpected(ETIMEDOUT);
  }

  TestResult result = future.get();
  if (!result.has_value()) {
    return std::unexpected(result.error());
  }
  return *std::move(result);
}

void forget_inode_best_effort(fdbfs_ino_t ino) noexcept {
  const auto generation = decrement_lookup_count(ino, 1);
  if (!generation.has_value()) {
    return;
  }

  try {
    auto req = make_test_request();
    auto future = take_future(req.get());
    auto *inflight = new Inflight_forget<TestInflightAction>(
        req.get(), std::vector<ForgetEntry>{{ino, *generation}},
        inflight_test_services().make_transaction());
    inflight->start();
    (void)wait_for_test_result(future);
  } catch (...) {
    best_effort_clear_inode_use_record(ino, *generation);
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

std::string unique_test_name(std::string_view category, std::string_view stem) {
  static uint64_t next_id = 1;
  return "inflight-" + std::string(category) + "-" +
         std::to_string(next_id++) + "-" + std::string(stem);
}

TestResult wait_test_result(PendingTestOp &op) {
  REQUIRE(op.future.wait_for(kInflightTestTimeout) == std::future_status::ready);
  return op.future.get();
}

std::expected<INodeRecord, int> wait_lookup_inode(PendingTestOp &op) {
  TestResult result = wait_test_result(op);
  if (!result.has_value()) {
    return std::unexpected(result.error());
  }
  REQUIRE(std::holds_alternative<TestReplyINode>(*result));
  return std::get<TestReplyINode>(*result).inode;
}

std::expected<INodeRecord, int> wait_getinode(PendingTestOp &op) {
  TestResult result = wait_test_result(op);
  if (!result.has_value()) {
    return std::unexpected(result.error());
  }
  REQUIRE(std::holds_alternative<TestReplyINode>(*result));
  return std::get<TestReplyINode>(*result).inode;
}

int wait_status(PendingTestOp &op) {
  TestResult result = wait_test_result(op);
  if (!result.has_value()) {
    return result.error();
  }
  REQUIRE(std::holds_alternative<TestReplyNone>(*result));
  return 0;
}

void forget_lookup_best_effort(fdbfs_ino_t ino) {
  forget_inode_best_effort(ino);
}

PendingTestOp start_lookup(fdbfs_ino_t parent, std::string_view name) {
  return start_test_op<Inflight_lookup<TestInflightAction,
                                       TestInflightAction::INodeHandlerEntry>>(
      parent, std::string(name), inflight_test_services().make_transaction(),
      TestInflightAction::INodeHandlerEntry{});
}

PendingTestOp start_lookup_root(std::string_view name) {
  return start_lookup(kTestRootIno, name);
}

PendingTestOp start_getinode_ino(fdbfs_ino_t ino) {
  return start_test_op<Inflight_getinode<TestInflightAction, std::monostate>>(
      ino, inflight_test_services().make_transaction(), std::monostate{});
}

PendingTestOp start_rename(std::string_view from, std::string_view to,
                           unsigned flags, fdbfs_ino_t oldparent,
                           fdbfs_ino_t newparent) {
  return start_test_op<Inflight_rename<TestInflightAction>>(
      oldparent, std::string(from), newparent, std::string(to), flags,
      inflight_test_services().make_transaction());
}

INodeRecord create_regular_file(std::string_view name, mode_t mode,
                                fdbfs_ino_t parent) {
  auto op = start_test_op<Inflight_mknod<TestInflightAction, std::monostate>>(
      parent, std::string(name), mode, ft_regular, 0,
      inflight_test_services().make_transaction(), std::nullopt,
      std::monostate{});
  auto created = wait_getinode(op);
  REQUIRE(created.has_value());
  return *std::move(created);
}

INodeRecord create_directory(std::string_view name, mode_t mode,
                             fdbfs_ino_t parent) {
  auto op = start_test_op<Inflight_mknod<TestInflightAction, std::monostate>>(
      parent, std::string(name), mode, ft_directory, 0,
      inflight_test_services().make_transaction(), std::nullopt,
      std::monostate{});
  auto created = wait_getinode(op);
  REQUIRE(created.has_value());
  return *std::move(created);
}

INodeRecord create_child_file(fdbfs_ino_t parent, std::string_view name,
                              mode_t mode) {
  return create_regular_file(name, mode, parent);
}

INodeRecord create_hardlink(fdbfs_ino_t ino, std::string_view name,
                            fdbfs_ino_t newparent) {
  auto op = start_test_op<Inflight_link<TestInflightAction, std::monostate>>(
      ino, newparent, std::string(name),
      inflight_test_services().make_transaction(), std::monostate{});
  auto linked = wait_getinode(op);
  REQUIRE(linked.has_value());
  return *std::move(linked);
}

int rename_name(std::string_view from, std::string_view to, unsigned flags,
                fdbfs_ino_t oldparent, fdbfs_ino_t newparent) {
  auto op = start_rename(from, to, flags, oldparent, newparent);
  return wait_status(op);
}

TestINode::TestINode(fdbfs_ino_t ino, INodeRecord inode, bool tracked)
    : ino_(ino), inode_(std::move(inode)), tracked_(tracked) {}

TestINode::~TestINode() { reset(); }

TestINode::TestINode(TestINode &&other) noexcept
    : ino_(other.ino_), inode_(std::move(other.inode_)),
      tracked_(other.tracked_) {
  other.ino_ = 0;
  other.tracked_ = false;
}

TestINode &TestINode::operator=(TestINode &&other) noexcept {
  if (this == &other) {
    return *this;
  }
  reset();
  ino_ = other.ino_;
  inode_ = std::move(other.inode_);
  tracked_ = other.tracked_;
  other.ino_ = 0;
  other.tracked_ = false;
  return *this;
}

bool TestINode::valid() const { return ino_ != 0; }

fdbfs_ino_t TestINode::ino() const { return ino_; }

const INodeRecord &TestINode::inode_record() const { return inode_; }

void TestINode::reset() noexcept {
  if (tracked_ && (ino_ != 0)) {
    forget_inode_best_effort(ino_);
  }
  ino_ = 0;
  inode_.Clear();
  tracked_ = false;
}

std::expected<TestINode, int> get_test_inode(fdbfs_ino_t ino) {
  auto req = make_test_request();
  auto future = take_future(req.get());

  auto *inflight = new Inflight_getinode<TestInflightAction, std::monostate>(
      req.get(), ino, inflight_test_services().make_transaction(),
      std::monostate{});
  inflight->start();

  auto reply = wait_for_test_result(future);
  if (!reply.has_value()) {
    return std::unexpected(reply.error());
  }
  if (!std::holds_alternative<TestReplyINode>(*reply)) {
    return std::unexpected(EIO);
  }

  INodeRecord inode = std::get<TestReplyINode>(*reply).inode;
  return TestINode(inode.inode(), std::move(inode), false);
}

std::expected<TestINode, int> lookup_test_inode(fdbfs_ino_t parent,
                                                std::string_view name) {
  auto req = make_test_request();
  auto future = take_future(req.get());

  auto *inflight = new Inflight_lookup<TestInflightAction,
                                       TestInflightAction::INodeHandlerEntry>(
      req.get(), parent, std::string(name),
      inflight_test_services().make_transaction(),
      TestInflightAction::INodeHandlerEntry{});
  inflight->start();

  auto reply = wait_for_test_result(future);
  if (!reply.has_value()) {
    return std::unexpected(reply.error());
  }
  if (!std::holds_alternative<TestReplyINode>(*reply)) {
    return std::unexpected(EIO);
  }

  INodeRecord inode = std::get<TestReplyINode>(*reply).inode;
  return TestINode(inode.inode(), std::move(inode), true);
}

std::expected<TestINode, int> resolve_test_path(std::string_view path) {
  if (path.empty() || (path == "/")) {
    return get_test_inode(1);
  }

  auto root = get_test_inode(1);
  if (!root.has_value()) {
    return std::unexpected(root.error());
  }
  TestINode current = std::move(*root);

  size_t pos = 0;
  while (pos < path.size()) {
    while ((pos < path.size()) && (path[pos] == '/')) {
      pos += 1;
    }
    if (pos >= path.size()) {
      break;
    }

    const size_t next_slash = path.find('/', pos);
    const std::string_view component =
        path.substr(pos, next_slash == std::string_view::npos
                             ? std::string_view::npos
                             : (next_slash - pos));
    if (component.empty() || (component == ".")) {
      pos = (next_slash == std::string_view::npos) ? path.size() : next_slash;
      continue;
    }
    if (component == "..") {
      return std::unexpected(EINVAL);
    }

    auto next = lookup_test_inode(current.ino(), component);
    if (!next.has_value()) {
      return std::unexpected(next.error());
    }
    current = std::move(*next);
    pos = (next_slash == std::string_view::npos) ? path.size() : next_slash;
  }

  return current;
}
