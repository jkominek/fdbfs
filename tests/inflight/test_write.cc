#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "generic/read.hpp"
#include "generic/write.hpp"
#include "test_inflight_action.h"
#include "test_support.h"

namespace {

std::vector<uint8_t> read_exact(fdbfs_ino_t ino, size_t size, off_t off) {
  auto op = start_test_op<Inflight_read<TestInflightAction>>(
      ino, size, off, inflight_test_services().make_transaction());
  TestResult result = wait_test_result(op);
  REQUIRE(result.has_value());
  REQUIRE(std::holds_alternative<TestReplyBuf>(*result));
  const auto &reply = std::get<TestReplyBuf>(*result);
  REQUIRE(reply.bytes.size() == size);
  return reply.bytes;
}

size_t write_payload(fdbfs_ino_t ino, WritePayload payload, off_t off) {
  auto op = start_test_op<Inflight_write<TestInflightAction>>(
      ino, std::move(payload), WritePosOffset{.off = off},
      inflight_test_services().make_transaction());
  TestResult result = wait_test_result(op);
  REQUIRE(result.has_value());
  REQUIRE(std::holds_alternative<TestReplyWrite>(*result));
  return std::get<TestReplyWrite>(*result).size;
}

} // namespace

TEST_CASE("Inflight_write bytes payload overwrites partial and full blocks",
          "[inflight][write][bytes]") {
  const std::string name = unique_test_name("write", "bytes");
  auto inode = create_regular_file(name);

  const size_t total_size = BLOCKSIZE * 3;
  std::vector<uint8_t> initial(total_size, 0x7b);
  REQUIRE(write_payload(inode.inode(),
                        WritePayloadBytes{.bytes = std::move(initial)},
                        0) == total_size);

  const off_t write_offset = 1;
  std::vector<uint8_t> payload(BLOCKSIZE * 2);
  for (size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<uint8_t>((i % 251) + 1);
  }

  const size_t written = write_payload(
      inode.inode(), WritePayloadBytes{.bytes = payload}, write_offset);
  CHECK(written == payload.size());

  auto actual = read_exact(inode.inode(), total_size, 0);
  REQUIRE(actual.size() == total_size);

  std::vector<uint8_t> expected(total_size, 0x7b);
  std::copy(payload.begin(), payload.end(), expected.begin() + write_offset);
  CHECK(actual == expected);
}

TEST_CASE("Inflight_write zero payload clears partial and full blocks",
          "[inflight][write][zeroes]") {
  const std::string name = unique_test_name("write", "zeroes");
  auto inode = create_regular_file(name);

  const size_t total_size = BLOCKSIZE * 3;
  std::vector<uint8_t> initial(total_size, 0x7b);
  REQUIRE(write_payload(inode.inode(),
                        WritePayloadBytes{.bytes = std::move(initial)},
                        0) == total_size);

  const off_t zero_offset = 1;
  const size_t zero_size = BLOCKSIZE * 2;
  REQUIRE(write_payload(inode.inode(), WritePayloadZeroes{.size = zero_size},
                        zero_offset) == zero_size);

  auto actual = read_exact(inode.inode(), total_size, 0);
  REQUIRE(actual.size() == total_size);

  std::vector<uint8_t> expected(total_size, 0x7b);
  std::fill(expected.begin() + zero_offset,
            expected.begin() + zero_offset + zero_size, uint8_t{0});

  CHECK(actual == expected);
}

TEST_CASE("Inflight_write WritePosEOF appends every payload contiguously",
          "[inflight][write][append]") {
  const std::string name = unique_test_name("write", "append");
  auto inode = create_regular_file(name);

  constexpr uint8_t kWriters = 8;
  std::mt19937 rng(0x5eed1234);
  std::uniform_int_distribution<size_t> size_dist(17, 257);

  std::map<uint8_t, size_t> expected_run_sizes;
  std::vector<PendingTestOp> ops;
  ops.reserve(kWriters);

  size_t total_size = 0;
  for (uint8_t value = 1; value <= kWriters; ++value) {
    const size_t run_size = size_dist(rng);
    expected_run_sizes.emplace(value, run_size);
    total_size += run_size;

    std::vector<uint8_t> payload(run_size, value);
    ops.push_back(start_test_op<Inflight_write<TestInflightAction>>(
        inode.inode(), WritePayloadBytes{.bytes = std::move(payload)},
        WritePosEOF{}, inflight_test_services().make_transaction()));
  }

  for (auto &op : ops) {
    TestResult result = wait_test_result(op);
    REQUIRE(result.has_value());
    REQUIRE(std::holds_alternative<TestReplyWrite>(*result));
  }

  auto final_inode = get_test_inode(inode.inode());
  REQUIRE(final_inode.has_value());
  REQUIRE(final_inode->inode_record().size() == total_size);

  auto actual = read_exact(inode.inode(), total_size, 0);

  std::map<uint8_t, size_t> observed_run_sizes;
  size_t i = 0;
  while (i < actual.size()) {
    const uint8_t value = actual[i];
    REQUIRE(value >= 1);
    REQUIRE(value <= kWriters);
    REQUIRE(!observed_run_sizes.contains(value));

    size_t j = i + 1;
    while ((j < actual.size()) && (actual[j] == value)) {
      ++j;
    }
    observed_run_sizes.emplace(value, j - i);
    i = j;
  }

  CHECK(observed_run_sizes == expected_run_sizes);
}
