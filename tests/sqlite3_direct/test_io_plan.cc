#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <random>
#include <vector>

#include "io_plan.h"

extern uint32_t BLOCKSIZE;

namespace {

uint64_t ceil_div(uint64_t n, uint64_t d) { return (n + d - 1) / d; }

uint64_t max_size_with_remainder(uint64_t remainder, size_t max_op_size) {
  if (remainder == 0) {
    return max_op_size;
  }
  return remainder +
         ((max_op_size - remainder) / BLOCKSIZE) * static_cast<uint64_t>(BLOCKSIZE);
}

uint64_t expected_num_reads(uint64_t iOfst, uint64_t iAmt, size_t max_op_size) {
  if (iAmt <= max_op_size) {
    return 1;
  }

  const uint64_t request_end = iOfst + iAmt;
  const uint64_t begin_mod = iOfst % BLOCKSIZE;
  const uint64_t end_mod = request_end % BLOCKSIZE;
  const uint64_t first_remainder =
      (begin_mod == 0) ? 0 : static_cast<uint64_t>(BLOCKSIZE) - begin_mod;
  const uint64_t last_remainder = end_mod;
  const uint64_t first_cap = max_size_with_remainder(first_remainder, max_op_size);
  const uint64_t last_cap = max_size_with_remainder(last_remainder, max_op_size);
  if (iAmt <= first_cap + last_cap) {
    return 2;
  }
  return 2 + ceil_div(iAmt - first_cap - last_cap, max_op_size);
}

} // namespace

TEST_CASE("sqlite3 io plan partitions requests across aligned chunks",
          "[sqlite3_direct][io_plan]") {
  std::mt19937_64 rng(0x5d3d2a91ULL);

  const std::vector<size_t> max_op_sizes = {
      64u * 1024u,
      128u * 1024u,
      512u * 1024u,
      1024u * 1024u,
  };

  const uint64_t random_offset =
      std::uniform_int_distribution<uint64_t>(2, BLOCKSIZE - 2)(rng);
  const uint64_t random_small_amt =
      std::uniform_int_distribution<uint64_t>(2, BLOCKSIZE - 2)(rng);

  std::vector<uint64_t> offsets = {
      0,
      1,
      BLOCKSIZE / 2,
      BLOCKSIZE - 1,
      random_offset,
      BLOCKSIZE,
      BLOCKSIZE + 1,
      BLOCKSIZE + BLOCKSIZE / 2,
      2 * BLOCKSIZE - 1,
      BLOCKSIZE + random_offset,
  };

  std::vector<uint64_t> amounts = {
      1,
      BLOCKSIZE - 1,
      random_small_amt,
      BLOCKSIZE,
  };
  std::uniform_int_distribution<uint64_t> large_amount_dist(1, 10u * 1024u * 1024u);
  for (size_t i = 0; i < 64; ++i) {
    amounts.push_back(large_amount_dist(rng));
  }

  for (size_t max_op_size : max_op_sizes) {
    INFO("max_op_size=" << max_op_size);
    for (uint64_t iOfst : offsets) {
      for (uint64_t iAmt : amounts) {
        INFO("iOfst=" << iOfst << ", iAmt=" << iAmt);
        auto plan = fdbfs_sqlite3_plan_io(static_cast<sqlite3_int64>(iOfst),
                                          static_cast<int>(iAmt), max_op_size);

        REQUIRE(!plan.empty());
        CHECK(plan.front().offset == iOfst);
        CHECK(plan.size() == expected_num_reads(iOfst, iAmt, max_op_size));

        uint64_t cursor = iOfst;
        uint64_t total = 0;
        for (size_t i = 0; i < plan.size(); ++i) {
          const SqliteIoChunk &chunk = plan[i];
          CHECK(chunk.offset == cursor);
          CHECK(chunk.size > 0);
          CHECK(chunk.size <= max_op_size);
          if (i > 0) {
            CHECK((chunk.offset % BLOCKSIZE) == 0);
          }
          if (i + 1 < plan.size()) {
            CHECK(((chunk.offset + chunk.size) % BLOCKSIZE) == 0);
          }
          cursor += chunk.size;
          total += chunk.size;
        }
        CHECK(total == iAmt);
        CHECK(cursor == iOfst + iAmt);

        if (plan.size() > 1) {
          const uint64_t request_end = iOfst + iAmt;
          const uint64_t begin_mod = iOfst % BLOCKSIZE;
          const uint64_t end_mod = request_end % BLOCKSIZE;
          const uint64_t first_remainder =
              (begin_mod == 0) ? 0 : static_cast<uint64_t>(BLOCKSIZE) - begin_mod;
          const uint64_t last_remainder = end_mod;

          uint64_t min_blocks = UINT64_MAX;
          uint64_t max_blocks = 0;
          for (size_t i = 0; i < plan.size(); ++i) {
            const bool is_first = (i == 0);
            const bool is_last = (i + 1 == plan.size());
            const uint64_t remainder =
                is_first ? first_remainder : (is_last ? last_remainder : 0);
            REQUIRE(plan[i].size >= remainder);
            const uint64_t blocks = (plan[i].size - remainder) / BLOCKSIZE;
            min_blocks = std::min(min_blocks, blocks);
            max_blocks = std::max(max_blocks, blocks);
          }
          CHECK(max_blocks - min_blocks <= 2);
        }
      }
    }
  }
}
