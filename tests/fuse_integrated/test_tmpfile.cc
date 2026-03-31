#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#define FUSE_USE_VERSION 35
#include <fuse3/fuse_lowlevel.h>
#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>

#include "test_support.h"

TEST_CASE("O_TMPFILE openat on mounted directory creates unnamed file",
          "[integration][tmpfile][open]") {
  scenario([&](FdbfsEnv &env) {
#if FUSE_VERSION < FUSE_MAKE_VERSION(3, 17)
    SKIP("tmpfile is only available with libfuse 3.17+");
    return;
#endif

    int dirfd = ::open(env.mnt.c_str(), O_RDONLY | O_DIRECTORY);
    FDBFS_REQUIRE_NONNEG(dirfd);

    int fd = ::openat(dirfd, ".", O_TMPFILE | O_RDWR, 0600);
    FDBFS_REQUIRE_NONNEG(fd);

    const std::string payload = "tmpfile payload";
    REQUIRE(::write(fd, payload.data(), payload.size()) ==
            static_cast<ssize_t>(payload.size()));

    std::string got(payload.size(), '\0');
    REQUIRE(::pread(fd, got.data(), got.size(), 0) ==
            static_cast<ssize_t>(got.size()));
    CHECK(got == payload);

    struct stat st{};
    FDBFS_REQUIRE_OK(::fstat(fd, &st));
    CHECK(S_ISREG(st.st_mode));
    CHECK(st.st_nlink == 0);
    CHECK(st.st_size == static_cast<off_t>(payload.size()));

    FDBFS_REQUIRE_OK(::close(fd));
    FDBFS_REQUIRE_OK(::close(dirfd));
  });
}
