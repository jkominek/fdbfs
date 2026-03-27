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
      const auto names = readdir_names_once(dir_ino, start_name);
      CHECK(std::find(names.begin(), names.end(), std::string(start_name)) ==
            names.end());
    }
  }

  SECTION("readdirplus") {
    for (std::string_view start_name : {"a", "b", "d", "f"}) {
      INFO("start_name=" << start_name);
      const auto names = readdirplus_names_once(dir_ino, start_name);
      CHECK(std::find(names.begin(), names.end(), std::string(start_name)) ==
            names.end());
    }
  }
}
