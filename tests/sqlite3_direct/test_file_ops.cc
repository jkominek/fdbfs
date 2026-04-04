#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <cstring>
#include <vector>
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

TEST_CASE("sqlite3 file methods can read more than one MiB in one call",
          "[sqlite3_direct][file]") {
  sqlite3_direct_reset_database();
  sqlite3_vfs *vfs = sqlite3_direct_vfs();

  auto file = sqlite3_open_file(vfs, "/large-read.db",
                                SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE);
  sqlite3_file *raw = file.file();

  constexpr size_t chunk_size = 128 * 1024;
  constexpr size_t num_chunks = 16;
  std::vector<std::byte> chunk(chunk_size);
  std::vector<std::byte> expected(chunk_size * 12);

  for (size_t i = 0; i < num_chunks; ++i) {
    std::fill(chunk.begin(), chunk.end(), std::byte(static_cast<unsigned char>(i)));
    REQUIRE(raw->pMethods->xWrite(
                raw, chunk.data(), static_cast<int>(chunk.size()),
                static_cast<sqlite3_int64>(i * chunk_size)) == SQLITE_OK);
    if (i < 12) {
      std::fill(expected.begin() + static_cast<std::ptrdiff_t>(i * chunk_size),
                expected.begin() + static_cast<std::ptrdiff_t>((i + 1) * chunk_size),
                std::byte(static_cast<unsigned char>(i)));
    }
  }

  std::vector<std::byte> got(expected.size());
  REQUIRE(raw->pMethods->xRead(raw, got.data(), static_cast<int>(got.size()), 0) ==
          SQLITE_OK);
  CHECK(got == expected);
}

TEST_CASE("sqlite3 file methods can write more than one hundred twenty eight KiB in one call",
          "[sqlite3_direct][file]") {
  sqlite3_direct_reset_database();
  sqlite3_vfs *vfs = sqlite3_direct_vfs();

  auto file = sqlite3_open_file(vfs, "/large-write.db",
                                SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE);
  sqlite3_file *raw = file.file();

  constexpr size_t write_size = 384 * 1024;
  std::vector<std::byte> payload(write_size);
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = std::byte(static_cast<unsigned char>(i % 251));
  }

  REQUIRE(raw->pMethods->xWrite(raw, payload.data(),
                                static_cast<int>(payload.size()), 17) ==
          SQLITE_OK);

  sqlite3_int64 size = -1;
  REQUIRE(raw->pMethods->xFileSize(raw, &size) == SQLITE_OK);
  CHECK(size == static_cast<sqlite3_int64>(17 + payload.size()));

  std::vector<std::byte> got(payload.size());
  REQUIRE(raw->pMethods->xRead(raw, got.data(), static_cast<int>(got.size()), 17) ==
          SQLITE_OK);
  CHECK(got == payload);
}
