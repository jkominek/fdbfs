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

    struct stat st{};
    FDBFS_REQUIRE_OK(::stat(p.c_str(), &st));
    REQUIRE(S_ISREG(st.st_mode));
    REQUIRE(static_cast<size_t>(st.st_size) == payload.size());
  });
}

TEST_CASE("O_APPEND writes always append regardless of file offset",
          "[integration][open][write][stat]") {
  // this is a really simple test case that just checks that
  // O_APPEND doesn't explode. it doesn't test that O_APPEND
  // writes atomically seek to EOF and write, which is the
  // interesting/tricky property.
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("append.txt");

    {
      int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
      FDBFS_REQUIRE_NONNEG(fd);
      constexpr char seed[] = "seed";
      const ssize_t wrote = ::write(fd, seed, sizeof(seed) - 1);
      REQUIRE(wrote == static_cast<ssize_t>(sizeof(seed) - 1));
      FDBFS_REQUIRE_OK(::close(fd));
    }

    int fd = ::open(p.c_str(), O_WRONLY | O_APPEND);
    FDBFS_REQUIRE_NONNEG(fd);

    FDBFS_REQUIRE_NONNEG(::lseek(fd, 0, SEEK_SET));
    constexpr char a[] = "A";
    REQUIRE(::write(fd, a, sizeof(a) - 1) == 1);

    FDBFS_REQUIRE_NONNEG(::lseek(fd, 0, SEEK_SET));
    constexpr char b[] = "B";
    REQUIRE(::write(fd, b, sizeof(b) - 1) == 1);
    FDBFS_REQUIRE_OK(::close(fd));

    char buf[16] = {};
    int rfd = ::open(p.c_str(), O_RDONLY);
    FDBFS_REQUIRE_NONNEG(rfd);
    const ssize_t n = ::read(rfd, buf, sizeof(buf));
    FDBFS_REQUIRE_OK(::close(rfd));
    REQUIRE(n == 6);
    REQUIRE(std::string(buf, static_cast<size_t>(n)) == "seedAB");
  });
}
