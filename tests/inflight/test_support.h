#pragma once

#include <expected>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "generic/util.h"
#include "test_inflight_action.h"

inline constexpr fdbfs_ino_t kTestRootIno = 1;

class InflightTestServices {
public:
  InflightTestServices();
  ~InflightTestServices();

  InflightTestServices(const InflightTestServices &) = delete;
  InflightTestServices &operator=(const InflightTestServices &) = delete;
  InflightTestServices(InflightTestServices &&) = delete;
  InflightTestServices &operator=(InflightTestServices &&) = delete;

  [[nodiscard]] unique_transaction make_transaction() const;

private:
  class Impl;
  Impl *impl_;
};

InflightTestServices &inflight_test_services();

class TestINode {
public:
  TestINode() = default;
  TestINode(INodeRecord inode, bool tracked);
  ~TestINode();

  TestINode(const TestINode &) = delete;
  TestINode &operator=(const TestINode &) = delete;
  TestINode(TestINode &&other) noexcept;
  TestINode &operator=(TestINode &&other) noexcept;

  [[nodiscard]] bool valid() const;
  [[nodiscard]] const INodeRecord &inode_record() const;
  void reset() noexcept;

private:
  INodeRecord inode_;
  bool tracked_ = false;
};

std::unique_ptr<TestRequest> make_test_request();
std::future<TestResult> take_future(TestRequest *req);

struct PendingTestOp {
  std::unique_ptr<TestRequest> req;
  std::future<TestResult> future;
};

template <typename InflightT, typename... Args>
PendingTestOp start_test_op(Args &&...args) {
  auto req = make_test_request();
  auto future = take_future(req.get());
  auto *inflight = new InflightT(req.get(), std::forward<Args>(args)...);
  inflight->start();
  return PendingTestOp{
      .req = std::move(req),
      .future = std::move(future),
  };
}

std::string unique_test_name(std::string_view category, std::string_view stem);
TestResult wait_test_result(PendingTestOp &op);
std::expected<INodeRecord, int> wait_lookup_inode(PendingTestOp &op);
std::expected<INodeRecord, int> wait_getinode(PendingTestOp &op);
int wait_status(PendingTestOp &op);
void forget_lookup_best_effort(fdbfs_ino_t ino);

PendingTestOp start_lookup(fdbfs_ino_t parent, std::string_view name);
PendingTestOp start_lookup_root(std::string_view name);
PendingTestOp start_getinode_ino(fdbfs_ino_t ino);
PendingTestOp start_rename(std::string_view from, std::string_view to,
                           unsigned flags = 0,
                           fdbfs_ino_t oldparent = kTestRootIno,
                           fdbfs_ino_t newparent = kTestRootIno);

INodeRecord create_regular_file(std::string_view name, mode_t mode = 0644,
                                fdbfs_ino_t parent = kTestRootIno);
INodeRecord create_directory(std::string_view name, mode_t mode = 0755,
                             fdbfs_ino_t parent = kTestRootIno);
INodeRecord create_child_file(fdbfs_ino_t parent, std::string_view name,
                              mode_t mode = 0644);
INodeRecord create_hardlink(fdbfs_ino_t ino, std::string_view name,
                            fdbfs_ino_t newparent = kTestRootIno);
int rename_name(std::string_view from, std::string_view to,
                unsigned flags = 0,
                fdbfs_ino_t oldparent = kTestRootIno,
                fdbfs_ino_t newparent = kTestRootIno);

std::expected<TestINode, int> get_test_inode(fdbfs_ino_t ino);
std::expected<TestINode, int> lookup_test_inode(fdbfs_ino_t parent,
                                                std::string_view name);
std::expected<TestINode, int> resolve_test_path(std::string_view path);

std::vector<std::string> readdir_names_once(fdbfs_ino_t ino,
                                            std::string_view start_name = {});
std::vector<std::string>
readdirplus_names_once(fdbfs_ino_t ino, std::string_view start_name = {});
