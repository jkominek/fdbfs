#include <catch2/catch_test_macros.hpp>

#include <linux/fs.h>

#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif

#include "test_support.h"
#include <cerrno>
#include <sstream>
#include <string>

namespace {

std::string describe_inode_record(const INodeRecord &inode) {
  std::ostringstream os;
  os << "{ino=" << inode.inode();
  if (inode.has_type()) {
    os << " type=" << inode.type();
  } else {
    os << " type=<missing>";
  }
  if (inode.has_parentinode()) {
    os << " parentinode=" << inode.parentinode();
  } else {
    os << " parentinode=<missing>";
  }
  if (inode.has_nlinks()) {
    os << " nlinks=" << inode.nlinks();
  } else {
    os << " nlinks=<missing>";
  }
  os << "}";
  return os.str();
}

std::string describe_lookup_result(fdbfs_ino_t parent, std::string_view name) {
  auto inode = lookup_test_inode(parent, name);
  std::ostringstream os;
  os << "lookup(parent=" << parent << ", name=" << name << ")=";
  if (!inode.has_value()) {
    os << "error(" << inode.error() << ")";
    return os.str();
  }
  os << describe_inode_record(inode->inode_record());
  return os.str();
}

std::string describe_getinode_result(fdbfs_ino_t ino) {
  auto inode = get_test_inode(ino);
  std::ostringstream os;
  os << "getinode(" << ino << ")=";
  if (!inode.has_value()) {
    os << "error(" << inode.error() << ")";
    return os.str();
  }
  os << describe_inode_record(inode->inode_record());
  return os.str();
}

TestINode require_lookup_inode(fdbfs_ino_t parent, std::string_view name) {
  auto inode = lookup_test_inode(parent, name);
  INFO(describe_lookup_result(parent, name));
  REQUIRE(inode.has_value());
  return std::move(*inode);
}

void check_directory_parentinode(fdbfs_ino_t ino, fdbfs_ino_t expected_parent) {
  auto inode = get_test_inode(ino);
  REQUIRE(inode.has_value());
  CHECK(inode->inode_record().type() == ft_directory);
  CHECK(inode->inode_record().has_parentinode());
  CHECK(inode->inode_record().parentinode() == expected_parent);
}

void check_file_has_no_parentinode(fdbfs_ino_t ino) {
  auto inode = get_test_inode(ino);
  REQUIRE(inode.has_value());
  CHECK(inode->inode_record().type() == ft_regular);
  CHECK(!inode->inode_record().has_parentinode());
}

} // namespace

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

TEST_CASE("Inflight_rename parentinode updates for directories",
          "[inflight][rename]") {
  SECTION("rename directory within same parent keeps parentinode") {
    const std::string src_name = unique_test_name("rename-parent", "src");
    const std::string dst_name = unique_test_name("rename-parent", "dst");

    auto src = create_directory(src_name);
    const auto src_ino = src.inode();

    REQUIRE(rename_name(src_name, dst_name) == 0);

    auto dst = require_lookup_inode(kTestRootIno, dst_name);
    CHECK(dst.inode_record().inode() == src_ino);
    check_directory_parentinode(dst.inode_record().inode(), kTestRootIno);
  }

  SECTION("moving directory between parents updates parentinode") {
    const std::string parent_a_name = unique_test_name("rename-parent", "pa");
    const std::string parent_b_name = unique_test_name("rename-parent", "pb");
    const std::string child_name = unique_test_name("rename-parent", "child");
    const std::string moved_name = unique_test_name("rename-parent", "moved");

    auto parent_a = create_directory(parent_a_name);
    auto parent_b = create_directory(parent_b_name);
    auto child = create_directory(child_name, 0755, parent_a.inode());
    const auto child_ino = child.inode();

    REQUIRE(rename_name(child_name, moved_name, 0, parent_a.inode(),
                        parent_b.inode()) == 0);

    auto moved = require_lookup_inode(parent_b.inode(), moved_name);
    CHECK(moved.inode_record().inode() == child_ino);
    check_directory_parentinode(moved.inode_record().inode(), parent_b.inode());
  }

  SECTION("exchange two directories in same parent keeps both parentinodes") {
    const std::string left_name = unique_test_name("rename-parent", "left");
    const std::string right_name = unique_test_name("rename-parent", "right");

    auto left = create_directory(left_name);
    auto right = create_directory(right_name);
    const auto left_ino = left.inode();
    const auto right_ino = right.inode();

    REQUIRE(rename_name(left_name, right_name, RENAME_EXCHANGE) == 0);

    auto left_after = require_lookup_inode(kTestRootIno, left_name);
    auto right_after = require_lookup_inode(kTestRootIno, right_name);

    CHECK(left_after.inode_record().inode() == right_ino);
    CHECK(right_after.inode_record().inode() == left_ino);
    check_directory_parentinode(left_after.inode_record().inode(), kTestRootIno);
    check_directory_parentinode(right_after.inode_record().inode(),
                                kTestRootIno);
  }

  SECTION("exchange two directories across parents updates both parentinodes") {
    const std::string parent_a_name =
        unique_test_name("rename-parent", "xpa");
    const std::string parent_b_name =
        unique_test_name("rename-parent", "xpb");
    const std::string left_name = unique_test_name("rename-parent", "xleft");
    const std::string right_name = unique_test_name("rename-parent", "xright");

    auto parent_a = create_directory(parent_a_name);
    auto parent_b = create_directory(parent_b_name);
    auto left = create_directory(left_name, 0755, parent_a.inode());
    auto right = create_directory(right_name, 0755, parent_b.inode());
    const auto left_ino = left.inode();
    const auto right_ino = right.inode();

    REQUIRE(rename_name(left_name, right_name, RENAME_EXCHANGE, parent_a.inode(),
                        parent_b.inode()) == 0);

    auto left_after = require_lookup_inode(parent_a.inode(), left_name);
    auto right_after = require_lookup_inode(parent_b.inode(), right_name);

    CHECK(left_after.inode_record().inode() == right_ino);
    CHECK(right_after.inode_record().inode() == left_ino);
    check_directory_parentinode(left_after.inode_record().inode(),
                                parent_a.inode());
    check_directory_parentinode(right_after.inode_record().inode(),
                                parent_b.inode());
  }

  SECTION("exchange file and directory in same parent keeps directory parent") {
    const std::string file_name = unique_test_name("rename-parent", "file");
    const std::string dir_name = unique_test_name("rename-parent", "dir");

    auto file = create_regular_file(file_name);
    auto dir = create_directory(dir_name);
    const auto file_ino = file.inode();
    const auto dir_ino = dir.inode();

    REQUIRE(rename_name(file_name, dir_name, RENAME_EXCHANGE) == 0);
    INFO(describe_lookup_result(kTestRootIno, file_name));
    INFO(describe_lookup_result(kTestRootIno, dir_name));
    INFO(describe_getinode_result(file_ino));
    INFO(describe_getinode_result(dir_ino));

    auto file_slot = require_lookup_inode(kTestRootIno, file_name);
    auto dir_slot = require_lookup_inode(kTestRootIno, dir_name);

    CHECK(file_slot.inode_record().inode() == dir_ino);
    CHECK(dir_slot.inode_record().inode() == file_ino);
    check_directory_parentinode(file_slot.inode_record().inode(), kTestRootIno);
    check_file_has_no_parentinode(dir_slot.inode_record().inode());
  }

  SECTION("exchange file and directory across parents updates only directory") {
    const std::string parent_a_name =
        unique_test_name("rename-parent", "fpa");
    const std::string parent_b_name =
        unique_test_name("rename-parent", "fpb");
    const std::string file_name = unique_test_name("rename-parent", "ffile");
    const std::string dir_name = unique_test_name("rename-parent", "fdir");

    auto parent_a = create_directory(parent_a_name);
    auto parent_b = create_directory(parent_b_name);
    auto file = create_regular_file(file_name, 0644, parent_a.inode());
    auto dir = create_directory(dir_name, 0755, parent_b.inode());
    const auto file_ino = file.inode();
    const auto dir_ino = dir.inode();

    REQUIRE(rename_name(file_name, dir_name, RENAME_EXCHANGE, parent_a.inode(),
                        parent_b.inode()) == 0);
    INFO(describe_lookup_result(parent_a.inode(), file_name));
    INFO(describe_lookup_result(parent_b.inode(), dir_name));
    INFO(describe_getinode_result(file_ino));
    INFO(describe_getinode_result(dir_ino));

    auto file_slot = require_lookup_inode(parent_a.inode(), file_name);
    auto dir_slot = require_lookup_inode(parent_b.inode(), dir_name);

    CHECK(file_slot.inode_record().inode() == dir_ino);
    CHECK(dir_slot.inode_record().inode() == file_ino);
    check_directory_parentinode(file_slot.inode_record().inode(),
                                parent_a.inode());
    check_file_has_no_parentinode(dir_slot.inode_record().inode());
  }

  SECTION("rename directory over empty destination updates parentinode") {
    const std::string parent_a_name =
        unique_test_name("rename-parent", "opa");
    const std::string parent_b_name =
        unique_test_name("rename-parent", "opb");
    const std::string src_name = unique_test_name("rename-parent", "osrc");
    const std::string dst_name = unique_test_name("rename-parent", "odst");

    auto parent_a = create_directory(parent_a_name);
    auto parent_b = create_directory(parent_b_name);
    auto src = create_directory(src_name, 0755, parent_a.inode());
    auto dst = create_directory(dst_name, 0755, parent_b.inode());
    const auto src_ino = src.inode();
    const auto dst_ino = dst.inode();

    REQUIRE(rename_name(src_name, dst_name, 0, parent_a.inode(),
                        parent_b.inode()) == 0);
    INFO(describe_lookup_result(parent_a.inode(), src_name));
    INFO(describe_lookup_result(parent_b.inode(), dst_name));
    INFO(describe_getinode_result(src_ino));
    INFO(describe_getinode_result(dst_ino));

    auto dst_after = require_lookup_inode(parent_b.inode(), dst_name);
    CHECK(dst_after.inode_record().inode() == src_ino);
    CHECK(dst_after.inode_record().inode() != dst_ino);
    check_directory_parentinode(dst_after.inode_record().inode(),
                                parent_b.inode());
  }

  SECTION("regular files never gain parentinode on rename or move") {
    const std::string parent_a_name =
        unique_test_name("rename-parent", "rpa");
    const std::string parent_b_name =
        unique_test_name("rename-parent", "rpb");
    const std::string file_name = unique_test_name("rename-parent", "file");
    const std::string same_parent_name =
        unique_test_name("rename-parent", "same");
    const std::string moved_name = unique_test_name("rename-parent", "moved");

    auto parent_a = create_directory(parent_a_name);
    auto parent_b = create_directory(parent_b_name);
    auto file = create_regular_file(file_name, 0644, parent_a.inode());
    const auto file_ino = file.inode();

    REQUIRE(rename_name(file_name, same_parent_name, 0, parent_a.inode(),
                        parent_a.inode()) == 0);
    INFO(describe_lookup_result(parent_a.inode(), file_name));
    INFO(describe_lookup_result(parent_a.inode(), same_parent_name));
    INFO(describe_getinode_result(file_ino));
    auto same_parent = require_lookup_inode(parent_a.inode(), same_parent_name);
    CHECK(same_parent.inode_record().inode() == file_ino);
    check_file_has_no_parentinode(same_parent.inode_record().inode());

    REQUIRE(rename_name(same_parent_name, moved_name, 0, parent_a.inode(),
                        parent_b.inode()) == 0);
    INFO(describe_lookup_result(parent_a.inode(), same_parent_name));
    INFO(describe_lookup_result(parent_b.inode(), moved_name));
    INFO(describe_getinode_result(file_ino));
    auto moved = require_lookup_inode(parent_b.inode(), moved_name);
    CHECK(moved.inode_record().inode() == file_ino);
    check_file_has_no_parentinode(moved.inode_record().inode());
  }
}
