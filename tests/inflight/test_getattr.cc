#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <ctime>
#include <sstream>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "generic/atomicfield.hpp"
#include "generic/read.hpp"
#include "generic/setattr.hpp"
#include "generic/unlink.hpp"
#include "generic/write.hpp"
#include "test_inflight_action.h"
#include "test_support.h"

namespace {

struct TimestampExpectations {
  bool atime_changed;
  bool mtime_changed;
  bool ctime_changed;
};

struct InodeSnapshot {
  INodeRecord inode;
  struct timespec atime{};
  struct timespec mtime{};
  struct timespec ctime{};
  Timespec atime_proto;
  Timespec mtime_proto;
  Timespec ctime_proto;
};

struct timespec require_timespec(const INodeRecord &inode,
                                 bool (INodeRecord::*has_field)() const,
                                 const Timespec &(INodeRecord::*field)()
                                     const) {
  REQUIRE((inode.*has_field)());
  const auto &ts = (inode.*field)();
  struct timespec out{};
  out.tv_sec = static_cast<decltype(out.tv_sec)>(ts.sec());
  out.tv_nsec = static_cast<decltype(out.tv_nsec)>(ts.nsec());
  return out;
}

InodeSnapshot require_inode_snapshot(fdbfs_ino_t ino) {
  auto inode = get_test_inode(ino);
  REQUIRE(inode.has_value());
  return InodeSnapshot{
      .inode = inode->inode_record(),
      .atime = require_timespec(inode->inode_record(), &INodeRecord::has_atime,
                                &INodeRecord::atime),
      .mtime = require_timespec(inode->inode_record(), &INodeRecord::has_mtime,
                                &INodeRecord::mtime),
      .ctime = require_timespec(inode->inode_record(), &INodeRecord::has_ctime,
                                &INodeRecord::ctime),
      .atime_proto = inode->inode_record().atime(),
      .mtime_proto = inode->inode_record().mtime(),
      .ctime_proto = inode->inode_record().ctime(),
  };
}

void check_single_time_transition(std::string_view name,
                                  const struct timespec &before,
                                  const struct timespec &after,
                                  const Timespec &before_proto,
                                  const Timespec &after_proto,
                                  bool expected_change) {
  const int cmp = compare_timespec_value(after, before);

  INFO(name << " before=" << before.tv_sec << "." << before.tv_nsec << " after="
            << after.tv_sec << "." << after.tv_nsec << " cmp=" << cmp);

  if (expected_change) {
    CHECK(cmp > 0);
  } else {
    CHECK(cmp == 0);
  }
}

void check_time_transition(std::string_view label, const InodeSnapshot &before,
                           const InodeSnapshot &after,
                           TimestampExpectations expectations) {
  INFO("transition=" << label);
  check_single_time_transition("atime", before.atime, after.atime,
                               before.atime_proto, after.atime_proto,
                               expectations.atime_changed);
  check_single_time_transition("mtime", before.mtime, after.mtime,
                               before.mtime_proto, after.mtime_proto,
                               expectations.mtime_changed);
  check_single_time_transition("ctime", before.ctime, after.ctime,
                               before.ctime_proto, after.ctime_proto,
                               expectations.ctime_changed);
}

void read_single_byte_and_flush_atime(fdbfs_ino_t ino) {
  auto read_op = start_test_op<Inflight_read<TestInflightAction>>(
      ino, static_cast<size_t>(1), static_cast<off_t>(0),
      inflight_test_services().make_transaction());
  TestResult read_result = wait_test_result(read_op);
  REQUIRE(read_result.has_value());
  REQUIRE(std::holds_alternative<TestReplyBuf>(*read_result));
  const auto &reply = std::get<TestReplyBuf>(*read_result);
  REQUIRE(reply.bytes.size() == 1);

  struct timespec now{};
  REQUIRE(::clock_gettime(CLOCK_REALTIME, &now) == 0);
  const auto encoded_now = encode_timespec(now);
  std::vector<AtomicFieldOp> ops;
  ops.push_back(AtomicFieldOp{
      .suffix = {'t', 'a'},
      .mutation_type = FDB_MUTATION_TYPE_MAX,
      .value = std::vector<uint8_t>(encoded_now.begin(), encoded_now.end()),
  });
  auto flush_op = start_test_op<Inflight_atomicfield<TestInflightAction>>(
      ino, std::move(ops), inflight_test_services().make_transaction());
  CHECK(wait_status(flush_op) == 0);
}

void write_single_byte(fdbfs_ino_t ino, off_t off, uint8_t value) {
  std::vector<uint8_t> buffer{value};
  auto write_op = start_test_op<Inflight_write<TestInflightAction>>(
      ino, WritePayloadBytes{.bytes = std::move(buffer)},
      WritePosOffset{.off = off},
      inflight_test_services().make_transaction());
  TestResult write_result = wait_test_result(write_op);
  REQUIRE(write_result.has_value());
  REQUIRE(std::holds_alternative<TestReplyWrite>(*write_result));
  CHECK(std::get<TestReplyWrite>(*write_result).size == 1);
}

void setattr_only(fdbfs_ino_t ino, const struct stat &attr, SetAttrMask mask) {
  auto setattr_op =
      start_test_op<Inflight_setattr<TestInflightAction, std::monostate>>(
          ino, attr, mask, inflight_test_services().make_transaction(),
          std::monostate{});
  auto updated = wait_getinode(setattr_op);
  REQUIRE(updated.has_value());
}

void chmod_inode(fdbfs_ino_t ino, mode_t mode) {
  struct stat attr{};
  attr.st_mode = mode;
  setattr_only(ino, attr, SetAttrMask(SetAttrBit::Mode));
}

void truncate_inode(fdbfs_ino_t ino, off_t size) {
  struct stat attr{};
  attr.st_size = size;
  setattr_only(ino, attr, SetAttrMask(SetAttrBit::Size));
}

int unlink_name(fdbfs_ino_t parent, std::string_view name) {
  auto unlink_op = start_test_op<Inflight_unlink_rmdir<TestInflightAction>>(
      parent, std::string(name), Op::Unlink,
      inflight_test_services().make_transaction());
  return wait_status(unlink_op);
}

void run_timestamp_behavior_matrix() {
  SECTION("regular file inode timestamps") {
    const std::string name = unique_test_name("getattr", "ts-file");
    const std::string alias = unique_test_name("getattr", "ts-file-link");

    auto created = create_regular_file(name);
    const fdbfs_ino_t ino = created.inode();

    write_single_byte(ino, 0, 'a');
    write_single_byte(ino, 1, 'b');
    write_single_byte(ino, 2, 'c');
    write_single_byte(ino, 3, 'd');

    InodeSnapshot before = require_inode_snapshot(ino);

    read_single_byte_and_flush_atime(ino);
    InodeSnapshot after_read = require_inode_snapshot(ino);
    check_time_transition("read", before, after_read,
                          TimestampExpectations{
                              .atime_changed = true,
                              .mtime_changed = false,
                              .ctime_changed = false,
                          });

    write_single_byte(ino, 1, 'Z');
    InodeSnapshot after_write = require_inode_snapshot(ino);
    check_time_transition("write", after_read, after_write,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });

    chmod_inode(ino, 0600);
    InodeSnapshot after_chmod = require_inode_snapshot(ino);
    check_time_transition("chmod", after_write, after_chmod,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = false,
                              .ctime_changed = true,
                          });

    auto linked = create_hardlink(ino, alias);
    REQUIRE(linked.inode() == ino);
    InodeSnapshot after_link = require_inode_snapshot(ino);
    check_time_transition("link", after_chmod, after_link,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = false,
                              .ctime_changed = true,
                          });
    CHECK(after_link.inode.nlinks() == 2);

    REQUIRE(unlink_name(kTestRootIno, name) == 0);
    InodeSnapshot after_unlink = require_inode_snapshot(ino);
    check_time_transition("unlink", after_link, after_unlink,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = false,
                              .ctime_changed = true,
                          });
    CHECK(after_unlink.inode.nlinks() == 1);

    truncate_inode(ino, 1);
    InodeSnapshot after_truncate = require_inode_snapshot(ino);
    check_time_transition("truncate", after_unlink, after_truncate,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });
    CHECK(after_truncate.inode.size() == 1);
  }

  SECTION("parent directory timestamps") {
    const std::string dir_name = unique_test_name("getattr", "ts-dir");
    const std::string dummy = unique_test_name("getattr", "ts-child-dummy");
    const std::string child_a = unique_test_name("getattr", "child-a");
    const std::string child_b = unique_test_name("getattr", "child-b");

    auto dir = create_directory(dir_name);
    const fdbfs_ino_t dir_ino = dir.inode();

    create_child_file(dir_ino, dummy);
    chmod_inode(dir_ino, 0755);

    InodeSnapshot before_create = require_inode_snapshot(dir_ino);
    auto child = create_child_file(dir_ino, child_a);
    chmod_inode(dir_ino, 0755);
    (void)child;
    InodeSnapshot after_create = require_inode_snapshot(dir_ino);
    check_time_transition("create child", before_create, after_create,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });

    InodeSnapshot before_rename = require_inode_snapshot(dir_ino);
    REQUIRE(rename_name(child_a, child_b, 0, dir_ino, dir_ino) == 0);
    chmod_inode(dir_ino, 0755);
    InodeSnapshot after_rename = require_inode_snapshot(dir_ino);
    check_time_transition("rename child", before_rename, after_rename,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });

    InodeSnapshot before_unlink = require_inode_snapshot(dir_ino);
    REQUIRE(unlink_name(dir_ino, child_b) == 0);
    chmod_inode(dir_ino, 0755);
    InodeSnapshot after_unlink = require_inode_snapshot(dir_ino);
    check_time_transition("unlink child", before_unlink, after_unlink,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });
  }

}

} // namespace

TEST_CASE(
    "Inflight getattr timestamp behavior matrix for files and directories",
    "[inflight][getattr][timestamps]") {
  inflight_test_services();
  run_timestamp_behavior_matrix();
}
