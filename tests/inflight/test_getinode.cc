#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <future>
#include <variant>

#include "generic/getinode.hpp"
#include "test_inflight_action.h"
#include "test_support.h"

TEST_CASE("Inflight_getinode fetches root inode record", "[inflight][getinode]") {
  auto &services = inflight_test_services();

  auto req = make_test_request();
  auto future = take_future(req.get());

  auto *inflight = new Inflight_getinode<TestInflightAction, std::monostate>(
      req.get(), 1, services.make_transaction(), std::monostate{});
  inflight->start();

  REQUIRE(future.wait_for(std::chrono::seconds(5)) == std::future_status::ready);

  TestResult result = future.get();
  REQUIRE(result.has_value());
  REQUIRE(std::holds_alternative<TestReplyINode>(*result));

  const auto &reply = std::get<TestReplyINode>(*result);
  CHECK(reply.inode.type() == ft_directory);
  CHECK(reply.inode.inode() == 1);
  CHECK(reply.inode.nlinks() >= 2);
  CHECK(reply.inode.size() == 0);
}
