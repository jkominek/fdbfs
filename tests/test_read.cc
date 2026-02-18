#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "test_support.h"

TEST_CASE("create write readback matches", "[integration][open][write][read]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("readback.bin");
    const std::string payload = "hello-from-catch2";
    std::vector<char> buf(payload.size(), '\0');

    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    REQUIRE(::write(fd, payload.data(), payload.size()) ==
            static_cast<ssize_t>(payload.size()));
    FDBFS_REQUIRE_OK(::close(fd));

    fd = ::open(p.c_str(), O_RDONLY);
    FDBFS_REQUIRE_NONNEG(fd);
    REQUIRE(::read(fd, buf.data(), buf.size()) ==
            static_cast<ssize_t>(payload.size()));
    FDBFS_REQUIRE_OK(::close(fd));

    REQUIRE(std::string(buf.begin(), buf.end()) == payload);
  });
}
