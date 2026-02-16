#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>

#include "test_posix_helpers.h"
#include "test_support.h"

TEST_CASE("setattr via chmod and truncate updates inode fields",
          "[integration][setattr][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("setattr_file");
    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 06755);
    REQUIRE(fd >= 0);

    const auto payload = make_pattern(7000, 0x12345678ull);
    write_all_fd(fd, payload.data(), payload.size());
    REQUIRE(::close(fd) == 0);

    struct stat before {};
    REQUIRE(::stat(p.c_str(), &before) == 0);

    REQUIRE(::chmod(p.c_str(), 0640) == 0);
    struct stat chmod_st {};
    REQUIRE(::stat(p.c_str(), &chmod_st) == 0);
    CHECK((chmod_st.st_mode & 0777) == 0640);

    ::sleep(1);
    REQUIRE(::truncate(p.c_str(), 2000) == 0);
    struct stat shrunk {};
    REQUIRE(::stat(p.c_str(), &shrunk) == 0);
    CHECK(shrunk.st_size == 2000);
    CHECK(compare_timespec(shrunk.st_mtim, before.st_mtim) > 0);
    CHECK(compare_timespec(shrunk.st_ctim, before.st_ctim) > 0);

    ::sleep(1);
    REQUIRE(::truncate(p.c_str(), 12000) == 0);
    struct stat grown {};
    REQUIRE(::stat(p.c_str(), &grown) == 0);
    CHECK(grown.st_size == 12000);

    fd = ::open(p.c_str(), O_RDONLY);
    REQUIRE(fd >= 0);
    const auto whole = pread_exact_fd(fd, static_cast<size_t>(grown.st_size), 0);
    REQUIRE(::close(fd) == 0);

    REQUIRE(whole.size() == static_cast<size_t>(grown.st_size));
    CHECK(std::memcmp(whole.data(), payload.data(), 2000) == 0);
    for (size_t i = 2000; i < whole.size(); i++) {
      if (whole[i] != 0) {
        FAIL("expanded truncate region was not zero-filled");
      }
    }
  });
}

TEST_CASE("setattr via utimensat updates atime and mtime",
          "[integration][setattr][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("utimens_file");
    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    struct timespec ts[2]{};
    ts[0].tv_sec = 1700000000;
    ts[0].tv_nsec = 123456789;
    ts[1].tv_sec = 1700000123;
    ts[1].tv_nsec = 987654321;

    REQUIRE(::utimensat(AT_FDCWD, p.c_str(), ts, 0) == 0);

    struct stat st {};
    REQUIRE(::stat(p.c_str(), &st) == 0);
    CHECK(st.st_atim.tv_sec == ts[0].tv_sec);
    CHECK(st.st_atim.tv_nsec == ts[0].tv_nsec);
    CHECK(st.st_mtim.tv_sec == ts[1].tv_sec);
    CHECK(st.st_mtim.tv_nsec == ts[1].tv_nsec);
  });
}

TEST_CASE("setattr through chown works when privileged",
          "[integration][setattr][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("chown_file");
    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    const uid_t uid = ::geteuid();
    const gid_t gid = ::getegid();

    errno = 0;
    if (::chown(p.c_str(), uid, gid) == -1) {
      if (errno == EPERM) {
        SKIP("chown is not permitted in this environment");
      }
      FAIL("unexpected chown failure");
    }

    struct stat st {};
    REQUIRE(::stat(p.c_str(), &st) == 0);
    CHECK(st.st_uid == uid);
    CHECK(st.st_gid == gid);
  });
}

TEST_CASE("setattr substantial update clears setuid/setgid on regular files",
          "[integration][setattr][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("suid_file");
    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 06755);
    REQUIRE(fd >= 0);
    const auto payload = make_pattern(1024, 0xabcdefu);
    write_all_fd(fd, payload.data(), payload.size());
    REQUIRE(::close(fd) == 0);

    REQUIRE(::chmod(p.c_str(), 06755) == 0);
    struct stat before {};
    REQUIRE(::stat(p.c_str(), &before) == 0);
    CHECK((before.st_mode & 06000) == 06000);

    REQUIRE(::truncate(p.c_str(), 100) == 0);

    struct stat after {};
    REQUIRE(::stat(p.c_str(), &after) == 0);
    CHECK(after.st_size == 100);
    CHECK((after.st_mode & 06000) == 0);
    CHECK((after.st_mode & 01777) == 0755);
  });
}
