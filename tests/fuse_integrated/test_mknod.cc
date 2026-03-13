#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>

#include "test_support.h"

TEST_CASE("mknod supports fifo and regular nodes", "[integration][mknod][stat]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path fifo_path = env.p("pipe1");
    FDBFS_REQUIRE_OK(::mkfifo(fifo_path.c_str(), 0640));

    struct stat st {};
    FDBFS_REQUIRE_OK(::lstat(fifo_path.c_str(), &st));
    CHECK(S_ISFIFO(st.st_mode));

    const fs::path reg_path = env.p("reg1");
    FDBFS_REQUIRE_OK(::mknod(reg_path.c_str(), S_IFREG | 0600, 0));
    FDBFS_REQUIRE_OK(::lstat(reg_path.c_str(), &st));
    CHECK(S_ISREG(st.st_mode));
  });
}

TEST_CASE("mknod character device behavior", "[integration][mknod][stat]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path chr_path = env.p("char0");
    errno = 0;
    const int rc = ::mknod(chr_path.c_str(), S_IFCHR | 0600, makedev(1, 7));
    if (rc == -1 && errno == EPERM && ::geteuid() != 0) {
      SUCCEED("character-device mknod requires privileges in this environment");
      return;
    }
    REQUIRE(rc == 0);

    struct stat st {};
    FDBFS_REQUIRE_OK(::lstat(chr_path.c_str(), &st));
    CHECK(S_ISCHR(st.st_mode));
    CHECK(major(st.st_rdev) == 1);
    CHECK(minor(st.st_rdev) == 7);
  });
}

TEST_CASE("mknod rejects unsupported block device type", "[integration][mknod]") {
  scenario([&](FdbfsEnv &env) {
    errno = 0;
    CHECK(::mknod(env.p("blk0").c_str(), S_IFBLK | 0600, makedev(7, 0)) == -1);
    FDBFS_CHECK_ERRNO(EPERM);
  });
}
