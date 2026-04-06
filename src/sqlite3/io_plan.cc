#include "sqlite3/io_plan.h"

#include <cassert>

extern uint32_t BLOCKSIZE;

namespace {
uint64_t align_up(uint64_t value, uint64_t blockSize) {
  return ((value + blockSize - 1) / blockSize) * blockSize;
}

uint64_t align_down(uint64_t value, uint64_t blockSize) {
  return (value / blockSize) * blockSize;
}

uint64_t ceil_div(uint64_t n, uint64_t d) { return (n + d - 1) / d; }
} // namespace

std::vector<SqliteIoChunk> fdbfs_sqlite3_plan_io(sqlite3_int64 iOfst, int iAmt,
                                                 size_t maxOpSize) {
  if (iAmt <= 0) {
    return {};
  }

  assert(iOfst >= 0);
  assert(maxOpSize > 0);
  assert(maxOpSize % BLOCKSIZE == 0);

  const uint64_t initialOffset = static_cast<uint64_t>(iOfst);
  const uint64_t totalSize = static_cast<uint64_t>(iAmt);

  std::vector<SqliteIoChunk> plan;

  uint64_t currentOffset = initialOffset;
  uint64_t remainingSize = totalSize;

  while (remainingSize > 0) {
    const uint64_t operationsRemaining = ceil_div(remainingSize, maxOpSize);

    if (operationsRemaining == 1) {
      plan.push_back({currentOffset, remainingSize});
      break;
    }

    const uint64_t minimumSize =
        remainingSize - (operationsRemaining - 1) * maxOpSize;

    const uint64_t idealSize = ceil_div(remainingSize, operationsRemaining);

    // Choose the next split as close to even as possible, but keep it on a
    // block boundary and leave enough room for the remaining chunks to fit.
    const uint64_t earliestNextOffset =
        align_up(currentOffset + minimumSize, BLOCKSIZE);

    const uint64_t latestNextOffset =
        align_down(currentOffset + maxOpSize, BLOCKSIZE);

    uint64_t nextOffset = align_down(currentOffset + idealSize, BLOCKSIZE);

    if (nextOffset < earliestNextOffset) {
      nextOffset = earliestNextOffset;
    }
    if (nextOffset > latestNextOffset) {
      nextOffset = latestNextOffset;
    }

    const uint64_t chunkSize = nextOffset - currentOffset;
    assert(chunkSize > 0);
    assert(chunkSize <= maxOpSize);

    plan.push_back({currentOffset, chunkSize});

    currentOffset = nextOffset;
    remainingSize -= chunkSize;
  }

  return plan;
}
