#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstring>
#include <sqlite3.h>

#include "test_support.h"

TEST_CASE("sqlite3 file methods write read and report size",
          "[sqlite3_direct][file]") {
  sqlite3_direct_reset_database();
  sqlite3_vfs *vfs = sqlite3_direct_vfs();

  auto file = sqlite3_open_file(vfs, "/roundtrip.db",
                                SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE);
  sqlite3_file *raw = file.file();

  const std::array<std::byte, 5> payload = {
      std::byte{'h'}, std::byte{'e'}, std::byte{'l'}, std::byte{'l'},
      std::byte{'o'},
  };
  REQUIRE(raw->pMethods->xWrite(raw, payload.data(),
                                static_cast<int>(payload.size()), 0) ==
          SQLITE_OK);

  sqlite3_int64 size = -1;
  REQUIRE(raw->pMethods->xFileSize(raw, &size) == SQLITE_OK);
  CHECK(size == static_cast<sqlite3_int64>(payload.size()));

  std::array<std::byte, 5> got{};
  REQUIRE(raw->pMethods->xRead(raw, got.data(), static_cast<int>(got.size()),
                               0) == SQLITE_OK);
  CHECK(got == payload);

  CHECK(raw->pMethods->xSync(raw, 0) == SQLITE_OK);
}

TEST_CASE("sqlite3 file reads beyond eof short-read and zero-fill",
          "[sqlite3_direct][file]") {
  sqlite3_direct_reset_database();
  sqlite3_vfs *vfs = sqlite3_direct_vfs();

  auto file = sqlite3_open_file(vfs, "/short-read.db",
                                SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE);
  sqlite3_file *raw = file.file();

  const char payload[] = "abc";
  REQUIRE(raw->pMethods->xWrite(raw, payload, 3, 0) == SQLITE_OK);

  char got[8];
  std::memset(got, '\xff', sizeof(got));
  CHECK(raw->pMethods->xRead(raw, got, sizeof(got), 0) ==
        SQLITE_IOERR_SHORT_READ);
  CHECK(std::memcmp(got, "abc\0\0\0\0\0", sizeof(got)) == 0);
}

TEST_CASE("sqlite3 file truncate shrinks file size",
          "[sqlite3_direct][file]") {
  sqlite3_direct_reset_database();
  sqlite3_vfs *vfs = sqlite3_direct_vfs();

  auto file = sqlite3_open_file(vfs, "/truncate.db",
                                SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE);
  sqlite3_file *raw = file.file();

  const char payload[] = "abcdef";
  REQUIRE(raw->pMethods->xWrite(raw, payload, 6, 0) == SQLITE_OK);
  REQUIRE(raw->pMethods->xTruncate(raw, 2) == SQLITE_OK);

  sqlite3_int64 size = -1;
  REQUIRE(raw->pMethods->xFileSize(raw, &size) == SQLITE_OK);
  CHECK(size == 2);

  char got[4];
  std::memset(got, '\xff', sizeof(got));
  CHECK(raw->pMethods->xRead(raw, got, sizeof(got), 0) ==
        SQLITE_IOERR_SHORT_READ);
  CHECK(std::memcmp(got, "ab\0\0", sizeof(got)) == 0);
}

