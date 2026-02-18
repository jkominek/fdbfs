#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>

#include "test_support.h"

TEST_CASE("create write close stat size", "[integration][open][write][stat]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("hello.txt");
    const std::string payload = "abc123456789";

    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);

    const ssize_t wrote = ::write(fd, payload.data(), payload.size());
    REQUIRE(wrote == static_cast<ssize_t>(payload.size()));
    FDBFS_REQUIRE_OK(::close(fd));

    struct stat st {};
    FDBFS_REQUIRE_OK(::stat(p.c_str(), &st));
    REQUIRE(S_ISREG(st.st_mode));
    REQUIRE(static_cast<size_t>(st.st_size) == payload.size());
  });
}
