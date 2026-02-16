#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>

#include "test_support.h"

TEST_CASE("mknod supports fifo and regular nodes", "[integration][mknod][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path fifo_path = env.p("pipe1");
    REQUIRE(::mkfifo(fifo_path.c_str(), 0640) == 0);

    struct stat st {};
    REQUIRE(::lstat(fifo_path.c_str(), &st) == 0);
    CHECK(S_ISFIFO(st.st_mode));

    const fs::path reg_path = env.p("reg1");
    REQUIRE(::mknod(reg_path.c_str(), S_IFREG | 0600, 0) == 0);
    REQUIRE(::lstat(reg_path.c_str(), &st) == 0);
    CHECK(S_ISREG(st.st_mode));
  });
}

TEST_CASE("mknod character device behavior", "[integration][mknod][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path chr_path = env.p("char0");
    errno = 0;
    const int rc = ::mknod(chr_path.c_str(), S_IFCHR | 0600, makedev(1, 7));
    if (rc == -1 && errno == EPERM && ::geteuid() != 0) {
      SKIP("character-device mknod requires privileges in this environment");
    }
    REQUIRE(rc == 0);

    struct stat st {};
    REQUIRE(::lstat(chr_path.c_str(), &st) == 0);
    CHECK(S_ISCHR(st.st_mode));
    CHECK(major(st.st_rdev) == 1);
    CHECK(minor(st.st_rdev) == 7);
  });
}

TEST_CASE("mknod rejects unsupported block device type", "[integration][mknod]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    errno = 0;
    CHECK(::mknod(env.p("blk0").c_str(), S_IFBLK | 0600, makedev(7, 0)) == -1);
    CHECK(errno == EPERM);
  });
}
