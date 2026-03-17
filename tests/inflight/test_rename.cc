#include <catch2/catch_test_macros.hpp>

#include <linux/fs.h>

#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif

#include "test_support.h"
#include <cerrno>
#include <string>

TEST_CASE("Inflight_rename basic and replacement behavior",
          "[inflight][rename]") {
  const std::string a_name = unique_test_name("rename", "a");
  const std::string b_name = unique_test_name("rename", "b");
  const std::string c_name = unique_test_name("rename", "c");

  auto a = create_regular_file(a_name);
  const auto a_ino = a.inode();

  REQUIRE(rename_name(a_name, b_name) == 0);

  auto lookup_a = start_lookup_root(a_name);
  auto lookup_b = start_lookup_root(b_name);

  auto a_after = wait_lookup_inode(lookup_a);
  auto b_after = wait_lookup_inode(lookup_b);

  REQUIRE(!a_after.has_value());
  CHECK(a_after.error() == ENOENT);
  REQUIRE(b_after.has_value());
  CHECK(b_after->inode() == a_ino);
  forget_lookup_best_effort(b_after->inode());

  auto c = create_regular_file(c_name);
  const auto c_ino = c.inode();

  REQUIRE(rename_name(b_name, c_name) == 0);

  auto lookup_b2 = start_lookup_root(b_name);
  auto lookup_c = start_lookup_root(c_name);

  auto b_after2 = wait_lookup_inode(lookup_b2);
  auto c_after = wait_lookup_inode(lookup_c);

  REQUIRE(!b_after2.has_value());
  CHECK(b_after2.error() == ENOENT);
  REQUIRE(c_after.has_value());
  CHECK(c_after->inode() == a_ino);
  CHECK(c_after->inode() != c_ino);
  forget_lookup_best_effort(c_after->inode());
}

TEST_CASE("Inflight_rename type and emptiness checks", "[inflight][rename]") {
  const std::string file1_name = unique_test_name("rename", "f1");
  const std::string file2_name = unique_test_name("rename", "f2");
  const std::string dir1_name = unique_test_name("rename", "d1");
  const std::string dir2_name = unique_test_name("rename", "d2");
  const std::string child_name = unique_test_name("rename", "child");

  auto file1 = create_regular_file(file1_name);
  auto file2 = create_regular_file(file2_name);
  auto dir1 = create_directory(dir1_name);
  auto dir2 = create_directory(dir2_name);
  auto child = create_child_file(dir2.inode(), child_name);
  (void)file1;
  (void)file2;
  (void)dir1;
  (void)child;

  auto rename_file_over_dir =
      start_rename(file1_name, dir1_name, 0, kTestRootIno, kTestRootIno);
  CHECK(wait_status(rename_file_over_dir) == EISDIR);

  auto rename_dir_over_file =
      start_rename(dir1_name, file2_name, 0, kTestRootIno, kTestRootIno);
  CHECK(wait_status(rename_dir_over_file) == ENOTDIR);

  auto rename_dir_over_nonempty_dir =
      start_rename(dir1_name, dir2_name, 0, kTestRootIno, kTestRootIno);
  CHECK(wait_status(rename_dir_over_nonempty_dir) == ENOTEMPTY);
}

TEST_CASE("Inflight_rename directory onto empty directory succeeds",
          "[inflight][rename]") {
  const std::string src_name = unique_test_name("rename", "srcdir");
  const std::string dst_name = unique_test_name("rename", "dstdir");

  auto src = create_directory(src_name);
  auto dst = create_directory(dst_name);
  const auto src_ino = src.inode();
  const auto dst_ino = dst.inode();

  REQUIRE(rename_name(src_name, dst_name) == 0);

  auto lookup_src = start_lookup_root(src_name);
  auto lookup_dst = start_lookup_root(dst_name);

  auto src_after = wait_lookup_inode(lookup_src);
  auto dst_after = wait_lookup_inode(lookup_dst);

  REQUIRE(!src_after.has_value());
  CHECK(src_after.error() == ENOENT);
  REQUIRE(dst_after.has_value());
  CHECK(dst_after->inode() == src_ino);
  CHECK(dst_after->inode() != dst_ino);
  CHECK(dst_after->type() == ft_directory);
  forget_lookup_best_effort(dst_after->inode());
}

TEST_CASE("Inflight_rename NOREPLACE and EXCHANGE", "[inflight][rename]") {
  const std::string left_name = unique_test_name("rename", "left");
  const std::string right_name = unique_test_name("rename", "right");

  auto left = create_regular_file(left_name);
  auto right = create_regular_file(right_name);
  const auto left_ino = left.inode();
  const auto right_ino = right.inode();

#ifdef RENAME_NOREPLACE
  auto noreplace = start_rename(left_name, right_name, RENAME_NOREPLACE,
                                kTestRootIno, kTestRootIno);
  CHECK(wait_status(noreplace) == EEXIST);
#endif

  REQUIRE(rename_name(left_name, right_name, RENAME_EXCHANGE) == 0);

  auto lookup_left = start_lookup_root(left_name);
  auto lookup_right = start_lookup_root(right_name);

  auto left_after_lookup = wait_lookup_inode(lookup_left);
  auto right_after_lookup = wait_lookup_inode(lookup_right);

  REQUIRE(left_after_lookup.has_value());
  REQUIRE(right_after_lookup.has_value());

  auto stat_left = start_getinode_ino(left_after_lookup->inode());
  auto stat_right = start_getinode_ino(right_after_lookup->inode());

  auto left_after = wait_getinode(stat_left);
  auto right_after = wait_getinode(stat_right);

  REQUIRE(left_after.has_value());
  REQUIRE(right_after.has_value());
  CHECK(left_after->inode() == right_ino);
  CHECK(right_after->inode() == left_ino);
  forget_lookup_best_effort(left_after_lookup->inode());
  forget_lookup_best_effort(right_after_lookup->inode());

#ifdef RENAME_WHITEOUT
  auto whiteout = start_rename(left_name, right_name, RENAME_WHITEOUT,
                               kTestRootIno, kTestRootIno);
  CHECK(wait_status(whiteout) == ENOSYS);
#endif
}

TEST_CASE("Inflight_rename over hardlink to same inode is a no-op",
          "[inflight][rename]") {
  const std::string a_name = unique_test_name("rename", "a");
  const std::string b_name = unique_test_name("rename", "b");

  auto a_created = create_regular_file(a_name);
  auto b_created = create_hardlink(a_created.inode(), b_name);
  (void)b_created;

  auto lookup_a_before = start_lookup_root(a_name);
  auto lookup_b_before = start_lookup_root(b_name);

  auto a_before_lookup = wait_lookup_inode(lookup_a_before);
  auto b_before_lookup = wait_lookup_inode(lookup_b_before);

  REQUIRE(a_before_lookup.has_value());
  REQUIRE(b_before_lookup.has_value());

  auto stat_a_before = start_getinode_ino(a_before_lookup->inode());
  auto stat_b_before = start_getinode_ino(b_before_lookup->inode());

  auto a_before = wait_getinode(stat_a_before);
  auto b_before = wait_getinode(stat_b_before);

  REQUIRE(a_before.has_value());
  REQUIRE(b_before.has_value());
  REQUIRE(a_before->inode() == b_before->inode());
  const auto before_ino = a_before->inode();
  forget_lookup_best_effort(a_before_lookup->inode());
  forget_lookup_best_effort(b_before_lookup->inode());

  REQUIRE(rename_name(a_name, b_name) == 0);

  auto lookup_a_after = start_lookup_root(a_name);
  auto lookup_b_after = start_lookup_root(b_name);

  auto a_after_lookup = wait_lookup_inode(lookup_a_after);
  auto b_after_lookup = wait_lookup_inode(lookup_b_after);

  REQUIRE(a_after_lookup.has_value());
  REQUIRE(b_after_lookup.has_value());

  auto stat_a_after = start_getinode_ino(a_after_lookup->inode());
  auto stat_b_after = start_getinode_ino(b_after_lookup->inode());

  auto a_after = wait_getinode(stat_a_after);
  auto b_after = wait_getinode(stat_b_after);

  REQUIRE(a_after.has_value());
  REQUIRE(b_after.has_value());
  CHECK(a_after->inode() == b_after->inode());
  CHECK(a_after->inode() == before_ino);
  CHECK(b_after->inode() == before_ino);
  forget_lookup_best_effort(a_after_lookup->inode());
  forget_lookup_best_effort(b_after_lookup->inode());
}
