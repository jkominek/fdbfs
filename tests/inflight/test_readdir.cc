#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <string>
#include <string_view>

#include "test_support.h"

TEST_CASE("Inflight readdir restart points exclude the starting filename",
          "[inflight][readdir][readdirplus]") {
  inflight_test_services();

  const std::string dir_name = unique_test_name("readdir", "restart-dir");
  auto dir = create_directory(dir_name);
  const fdbfs_ino_t dir_ino = dir.inode();

  for (std::string_view name : {"a", "b", "c", "d", "e", "f"}) {
    create_child_file(dir_ino, name);
  }

  SECTION("readdir") {
    for (std::string_view start_name : {"a", "b", "d", "f"}) {
      INFO("start_name=" << start_name);
      const auto names =
          readdir_names_once(dir_ino, ReaddirStartKind::AfterName, start_name);
      CHECK(std::find(names.begin(), names.end(), std::string(start_name)) ==
            names.end());
    }
  }

  SECTION("readdirplus") {
    for (std::string_view start_name : {"a", "b", "d", "f"}) {
      INFO("start_name=" << start_name);
      const auto names =
          readdirplus_names_once(dir_ino, ReaddirStartKind::AfterName,
                                 start_name);
      CHECK(std::find(names.begin(), names.end(), std::string(start_name)) ==
            names.end());
    }
  }
}

TEST_CASE("Inflight readdir includes dot entries when appropriate",
          "[inflight][readdir][dot]") {
  inflight_test_services();

  SECTION("root beginning includes dot and dotdot") {
    const auto names =
        readdir_names_once(kTestRootIno, ReaddirStartKind::Beginning);
    REQUIRE(names.size() >= 2);
    CHECK(names[0] == ".");
    CHECK(names[1] == "..");
  }

  SECTION("nested directory beginning includes dot and dotdot first") {
    const std::string parent_name = unique_test_name("readdir", "dot-parent");
    const std::string child_name = unique_test_name("readdir", "dot-child");
    auto parent = create_directory(parent_name);
    auto child = create_directory(child_name, 0755, parent.inode());
    create_child_file(child.inode(), " leading-space");
    create_child_file(child.inode(), "-dash");
    create_child_file(child.inode(), ",comma");
    create_child_file(child.inode(), ".-between");
    create_child_file(child.inode(), "a");
    create_child_file(child.inode(), "b");

    const auto names =
        readdir_names_once(child.inode(), ReaddirStartKind::Beginning);
    REQUIRE(names.size() >= 8);
    CHECK(names[0] == ".");
    CHECK(names[1] == "..");
    CHECK(std::find(names.begin() + 2, names.end(), " leading-space") !=
          names.end());
    CHECK(std::find(names.begin() + 2, names.end(), "-dash") != names.end());
    CHECK(std::find(names.begin() + 2, names.end(), ",comma") != names.end());
    CHECK(std::find(names.begin() + 2, names.end(), ".-between") !=
          names.end());
    CHECK(std::find(names.begin() + 2, names.end(), "a") != names.end());
    CHECK(std::find(names.begin() + 2, names.end(), "b") != names.end());
  }

  SECTION("after dot starts with dotdot") {
    const std::string parent_name = unique_test_name("readdir", "afterdot-p");
    const std::string child_name = unique_test_name("readdir", "afterdot-c");
    auto parent = create_directory(parent_name);
    auto child = create_directory(child_name, 0755, parent.inode());
    create_child_file(child.inode(), "x");

    const auto names =
        readdir_names_once(child.inode(), ReaddirStartKind::AfterDot);
    REQUIRE(!names.empty());
    CHECK(names[0] == "..");
    CHECK(std::find(names.begin(), names.end(), ".") == names.end());
  }

  SECTION("after dotdot skips both synthetic entries") {
    const std::string parent_name = unique_test_name("readdir", "afterdotdot-p");
    const std::string child_name = unique_test_name("readdir", "afterdotdot-c");
    auto parent = create_directory(parent_name);
    auto child = create_directory(child_name, 0755, parent.inode());
    create_child_file(child.inode(), "x");
    create_child_file(child.inode(), "y");

    const auto names =
        readdir_names_once(child.inode(), ReaddirStartKind::AfterDotDot);
    CHECK(std::find(names.begin(), names.end(), ".") == names.end());
    CHECK(std::find(names.begin(), names.end(), "..") == names.end());
    CHECK(std::find(names.begin(), names.end(), "x") != names.end());
    CHECK(std::find(names.begin(), names.end(), "y") != names.end());
  }
}

TEST_CASE("Inflight readdirplus includes dot entries when appropriate",
          "[inflight][readdirplus][dot]") {
  inflight_test_services();

  SECTION("root beginning includes dot and dotdot") {
    const auto names =
        readdirplus_names_once(kTestRootIno, ReaddirStartKind::Beginning);
    REQUIRE(names.size() >= 2);
    CHECK(names[0] == ".");
    CHECK(names[1] == "..");
  }

  SECTION("nested directory beginning includes dot and dotdot first") {
    const std::string parent_name =
        unique_test_name("readdirplus", "dot-parent");
    const std::string child_name =
        unique_test_name("readdirplus", "dot-child");
    auto parent = create_directory(parent_name);
    auto child = create_directory(child_name, 0755, parent.inode());
    create_child_file(child.inode(), " leading-space");
    create_child_file(child.inode(), "-dash");
    create_child_file(child.inode(), ",comma");
    create_child_file(child.inode(), ".-between");
    create_child_file(child.inode(), "a");
    create_child_file(child.inode(), "b");

    const auto names =
        readdirplus_names_once(child.inode(), ReaddirStartKind::Beginning);
    REQUIRE(names.size() >= 8);
    CHECK(names[0] == ".");
    CHECK(names[1] == "..");
    CHECK(std::find(names.begin() + 2, names.end(), " leading-space") !=
          names.end());
    CHECK(std::find(names.begin() + 2, names.end(), "-dash") != names.end());
    CHECK(std::find(names.begin() + 2, names.end(), ",comma") != names.end());
    CHECK(std::find(names.begin() + 2, names.end(), ".-between") !=
          names.end());
    CHECK(std::find(names.begin() + 2, names.end(), "a") != names.end());
    CHECK(std::find(names.begin() + 2, names.end(), "b") != names.end());
  }

  SECTION("after dot starts with dotdot") {
    const std::string parent_name =
        unique_test_name("readdirplus", "afterdot-p");
    const std::string child_name =
        unique_test_name("readdirplus", "afterdot-c");
    auto parent = create_directory(parent_name);
    auto child = create_directory(child_name, 0755, parent.inode());
    create_child_file(child.inode(), "x");

    const auto names =
        readdirplus_names_once(child.inode(), ReaddirStartKind::AfterDot);
    REQUIRE(!names.empty());
    CHECK(names[0] == "..");
    CHECK(std::find(names.begin(), names.end(), ".") == names.end());
  }

  SECTION("after dotdot skips both synthetic entries") {
    const std::string parent_name =
        unique_test_name("readdirplus", "afterdotdot-p");
    const std::string child_name =
        unique_test_name("readdirplus", "afterdotdot-c");
    auto parent = create_directory(parent_name);
    auto child = create_directory(child_name, 0755, parent.inode());
    create_child_file(child.inode(), "x");
    create_child_file(child.inode(), "y");

    const auto names =
        readdirplus_names_once(child.inode(), ReaddirStartKind::AfterDotDot);
    CHECK(std::find(names.begin(), names.end(), ".") == names.end());
    CHECK(std::find(names.begin(), names.end(), "..") == names.end());
    CHECK(std::find(names.begin(), names.end(), "x") != names.end());
    CHECK(std::find(names.begin(), names.end(), "y") != names.end());
  }
}
