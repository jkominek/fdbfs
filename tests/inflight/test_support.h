#pragma once

#include <future>

#include "generic/util.h"
#include "test_inflight_action.h"

class InflightTestServices {
public:
  InflightTestServices();
  ~InflightTestServices();

  InflightTestServices(const InflightTestServices &) = delete;
  InflightTestServices &operator=(const InflightTestServices &) = delete;
  InflightTestServices(InflightTestServices &&) = delete;
  InflightTestServices &operator=(InflightTestServices &&) = delete;

  [[nodiscard]] unique_transaction make_transaction() const;

private:
  class Impl;
  Impl *impl_;
};

InflightTestServices &inflight_test_services();

std::unique_ptr<TestRequest> make_test_request();
std::future<TestResult> take_future(TestRequest *req);
