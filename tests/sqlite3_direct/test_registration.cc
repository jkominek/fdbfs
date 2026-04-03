#include <catch2/catch_test_macros.hpp>

#include "test_support.h"

TEST_CASE("sqlite3 extension init registers fdbfs vfs",
          "[sqlite3_direct][registration]") {
  sqlite3_vfs *vfs = sqlite3_direct_vfs();
  REQUIRE(vfs != nullptr);
  CHECK(std::string(vfs->zName) == "fdbfs");
  CHECK(vfs->xOpen != nullptr);
  CHECK(vfs->xDelete != nullptr);
  CHECK(vfs->xAccess != nullptr);
  CHECK(vfs->xFullPathname != nullptr);
}

