#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include <sqlite3.h>

struct SqliteIoChunk {
  uint64_t offset;
  size_t size;
};

std::vector<SqliteIoChunk> fdbfs_sqlite3_plan_io(
    sqlite3_int64 iOfst, int iAmt, size_t maxOpSize);
