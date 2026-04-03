#include <catch2/catch_test_macros.hpp>

#include <sqlite3.h>

#include "test_support.h"

TEST_CASE("sqlite3 vfs normalizes full paths", "[sqlite3_direct][vfs][path]") {
  sqlite3_direct_reset_database();
  sqlite3_vfs *vfs = sqlite3_direct_vfs();

  CHECK(sqlite3_full_path("foo.db") == "/foo.db");
  CHECK(sqlite3_full_path("/dir/../foo.db") == "/foo.db");

  char out[4] = {};
  CHECK(vfs->xFullPathname(vfs, "/toolong.db", sizeof(out), out) ==
        SQLITE_ERROR);
}

TEST_CASE("sqlite3 vfs access and delete reflect file presence",
          "[sqlite3_direct][vfs][path]") {
  sqlite3_direct_reset_database();
  sqlite3_vfs *vfs = sqlite3_direct_vfs();

  CHECK(sqlite3_access(vfs, "/main.db", SQLITE_ACCESS_EXISTS) == 0);

  {
    int out_flags = 0;
    auto file = sqlite3_open_file(vfs, "/main.db",
                                  SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE,
                                  &out_flags);
    CHECK(out_flags == (SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE));
  }

  CHECK(sqlite3_access(vfs, "/main.db", SQLITE_ACCESS_EXISTS) == 1);
  CHECK(sqlite3_access(vfs, "/main.db", SQLITE_ACCESS_READ) == 1);
  CHECK(sqlite3_access(vfs, "/main.db", SQLITE_ACCESS_READWRITE) == 1);

  CHECK(sqlite3_delete(vfs, "/main.db") == SQLITE_OK);
  CHECK(sqlite3_access(vfs, "/main.db", SQLITE_ACCESS_EXISTS) == 0);
}

TEST_CASE("sqlite3 vfs open without create fails for missing file",
          "[sqlite3_direct][vfs][open]") {
  sqlite3_direct_reset_database();
  sqlite3_vfs *vfs = sqlite3_direct_vfs();

  Sqlite3DirectFile file(vfs);
  CHECK(vfs->xOpen(vfs, "/missing.db", file.file(), SQLITE_OPEN_READWRITE,
                   nullptr) == SQLITE_CANTOPEN);
  CHECK(file.file()->pMethods == nullptr);
  CHECK(sqlite3_last_error(vfs) == "!target.has_value()");
}

TEST_CASE("sqlite3 vfs last error reports argument checks",
          "[sqlite3_direct][vfs][errors]") {
  sqlite3_direct_reset_database();
  sqlite3_vfs *vfs = sqlite3_direct_vfs();

  CHECK(vfs->xFullPathname(vfs, nullptr, 16, nullptr) == SQLITE_CANTOPEN);
  CHECK(sqlite3_last_error(vfs) == "zName==nullptr");
}
