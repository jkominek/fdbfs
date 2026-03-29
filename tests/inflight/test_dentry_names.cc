#include <catch2/catch_test_macros.hpp>

#include "generic/link.hpp"
#include "generic/mknod.hpp"
#include "generic/unlink.hpp"
#include "test_support.h"

namespace {

int mknod_status(std::string_view name, filetype type = ft_regular,
                 fdbfs_ino_t parent = kTestRootIno) {
  auto op = start_test_op<Inflight_mknod<TestInflightAction, std::monostate>>(
      parent, std::string(name), 0755, type, 0,
      inflight_test_services().make_transaction(), std::nullopt,
      std::monostate{});
  return wait_status(op);
}

int link_status(fdbfs_ino_t ino, fdbfs_ino_t newparent, std::string_view name) {
  auto op = start_test_op<Inflight_link<TestInflightAction, std::monostate>>(
      ino, newparent, std::string(name),
      inflight_test_services().make_transaction(), std::monostate{});
  return wait_status(op);
}

int unlink_status(fdbfs_ino_t parent, std::string_view name) {
  auto op = start_test_op<Inflight_unlink_rmdir<TestInflightAction>>(
      parent, std::string(name), Op::Unlink,
      inflight_test_services().make_transaction());
  return wait_status(op);
}

int rmdir_status(fdbfs_ino_t parent, std::string_view name) {
  auto op = start_test_op<Inflight_unlink_rmdir<TestInflightAction>>(
      parent, std::string(name), Op::Rmdir,
      inflight_test_services().make_transaction());
  return wait_status(op);
}

} // namespace

TEST_CASE("Inflight_lookup handles dot and dotdot", "[inflight][lookup][dentry]") {
  auto root_self = lookup_test_inode(kTestRootIno, ".");
  auto root_parent = lookup_test_inode(kTestRootIno, "..");
  REQUIRE(root_self.has_value());
  REQUIRE(root_parent.has_value());
  CHECK(root_self->inode_record().inode() == kTestRootIno);
  CHECK(root_parent->inode_record().inode() == kTestRootIno);
  CHECK(root_self->inode_record().type() == ft_directory);
  CHECK(root_parent->inode_record().type() == ft_directory);

  const std::string parent_name = unique_test_name("dentry", "parent");
  const std::string child_name = unique_test_name("dentry", "child");
  const std::string grandchild_name = unique_test_name("dentry", "grandchild");

  auto parent = create_directory(parent_name);
  auto child = create_directory(child_name, 0755, parent.inode());
  auto grandchild = create_directory(grandchild_name, 0755, child.inode());

  auto child_self = lookup_test_inode(child.inode(), ".");
  auto child_parent = lookup_test_inode(child.inode(), "..");
  auto grandchild_parent = lookup_test_inode(grandchild.inode(), "..");

  REQUIRE(child_self.has_value());
  REQUIRE(child_parent.has_value());
  REQUIRE(grandchild_parent.has_value());
  CHECK(child_self->inode_record().inode() == child.inode());
  CHECK(child_parent->inode_record().inode() == parent.inode());
  CHECK(grandchild_parent->inode_record().inode() == child.inode());
}

TEST_CASE("Inflight_mknod rejects dot and dotdot names",
          "[inflight][mknod][dentry]") {
  CHECK(mknod_status(".") == EEXIST);
  CHECK(mknod_status("..") == EEXIST);
}

TEST_CASE("Inflight_link rejects dot and dotdot destination names",
          "[inflight][link][dentry]") {
  const std::string file_name = unique_test_name("dentry", "file");
  auto file = create_regular_file(file_name);

  CHECK(link_status(file.inode(), kTestRootIno, ".") == EEXIST);
  CHECK(link_status(file.inode(), kTestRootIno, "..") == EEXIST);
}

TEST_CASE("Inflight_unlink_rmdir handles dot and dotdot names",
          "[inflight][unlink][rmdir][dentry]") {
  const std::string parent_name = unique_test_name("dentry", "rm-parent");
  const std::string child_name = unique_test_name("dentry", "rm-child");

  auto parent = create_directory(parent_name);
  auto child = create_directory(child_name, 0755, parent.inode());

  CHECK(unlink_status(kTestRootIno, ".") == EISDIR);
  CHECK(unlink_status(kTestRootIno, "..") == EISDIR);

  CHECK(rmdir_status(parent.inode(), ".") == EINVAL);
  CHECK(rmdir_status(child.inode(), "..") == ENOTEMPTY);
}

TEST_CASE("Inflight_rename rejects dot and dotdot names",
          "[inflight][rename][dentry]") {
  const std::string file_name = unique_test_name("dentry", "rename-file");
  const std::string other_name = unique_test_name("dentry", "rename-other");

  auto file = create_regular_file(file_name);
  (void)file;

  CHECK(rename_name(".", other_name) == EBUSY);
  CHECK(rename_name("..", other_name) == EBUSY);
  CHECK(rename_name(file_name, ".") == EBUSY);
  CHECK(rename_name(file_name, "..") == EBUSY);
}
