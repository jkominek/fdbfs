#include <catch2/catch_test_macros.hpp>

#include "generic/setattr.hpp"
#include "test_support.h"

namespace {

TestResult setattr_size_result(fdbfs_ino_t ino, off_t size) {
  struct stat attr {};
  attr.st_size = size;
  auto op =
      start_test_op<Inflight_setattr<TestInflightAction, std::monostate>>(
          ino, attr, SetAttrMask(SetAttrBit::Size),
          inflight_test_services().make_transaction(), std::monostate{});
  return wait_test_result(op);
}

} // namespace

TEST_CASE("Inflight_setattr does not allow truncating symlinks",
          "[inflight][setattr][symlink]") {
  const std::string target_name = unique_test_name("setattr-symlink", "target");
  const std::string link_name = unique_test_name("setattr-symlink", "link");

  auto target = create_regular_file(target_name);
  auto link = create_symlink(link_name, target_name);

  auto before_link = get_test_inode(link.inode());
  auto before_target = get_test_inode(target.inode());
  REQUIRE(before_link.has_value());
  REQUIRE(before_target.has_value());
  REQUIRE(before_link->inode_record().type() == ft_symlink);
  REQUIRE(before_link->inode_record().has_symlink());
  const auto before_link_size = before_link->inode_record().size();
  const auto before_target_size = before_target->inode_record().size();

  TestResult result = setattr_size_result(link.inode(), 1);
  REQUIRE(!result.has_value());

  auto after_link = get_test_inode(link.inode());
  auto after_target = get_test_inode(target.inode());
  REQUIRE(after_link.has_value());
  REQUIRE(after_target.has_value());

  CHECK(after_link->inode_record().type() == ft_symlink);
  CHECK(after_link->inode_record().has_symlink());
  CHECK(after_link->inode_record().symlink() == target_name);
  CHECK(after_link->inode_record().size() == before_link_size);
  CHECK(after_target->inode_record().size() == before_target_size);
}
