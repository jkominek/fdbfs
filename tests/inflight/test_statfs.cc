#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <future>
#include <sys/statvfs.h>
#include <variant>

#include "generic/statfs.hpp"
#include "test_inflight_action.h"
#include "test_support.h"

TEST_CASE("Inflight_statfs returns plausible statvfs data", "[inflight][statfs]") {
  auto &services = inflight_test_services();
  auto req = make_test_request();
  auto future = take_future(req.get());

  auto *inflight = new Inflight_statfs<TestInflightAction>(
      req.get(), services.make_transaction());
  inflight->start();

  REQUIRE(future.wait_for(std::chrono::seconds(5)) == std::future_status::ready);

  TestResult result = future.get();
  REQUIRE(result.has_value());
  REQUIRE(std::holds_alternative<TestReplyStatfs>(*result));

  const auto &reply = std::get<TestReplyStatfs>(*result);
  CHECK(reply.statbuf.f_bsize == BLOCKSIZE);
  CHECK(reply.statbuf.f_frsize == BLOCKSIZE);
  CHECK(reply.statbuf.f_blocks > 0);
  CHECK(reply.statbuf.f_bfree > 0);
  CHECK(reply.statbuf.f_bavail > 0);
  CHECK(reply.statbuf.f_blocks >= reply.statbuf.f_bfree);
  CHECK(reply.statbuf.f_blocks >= reply.statbuf.f_bavail);
  CHECK(reply.statbuf.f_files > 0);
  CHECK(reply.statbuf.f_ffree > 0);
  CHECK(reply.statbuf.f_favail > 0);
  CHECK(reply.statbuf.f_namemax >= 255);
}
