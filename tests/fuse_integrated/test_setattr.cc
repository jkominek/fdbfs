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
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("setattr_file");
    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 06755);
    FDBFS_REQUIRE_NONNEG(fd);

    const auto payload =
        generate_bytes(7000, BytePattern::Random, 0, 0x12345678ull);
    write_all_fd(fd, payload.data(), payload.size());
    FDBFS_REQUIRE_OK(::close(fd));

    struct stat before {};
    FDBFS_REQUIRE_OK(::stat(p.c_str(), &before));

    FDBFS_REQUIRE_OK(::chmod(p.c_str(), 0640));
    struct stat chmod_st {};
    FDBFS_REQUIRE_OK(::stat(p.c_str(), &chmod_st));
    CHECK((chmod_st.st_mode & 0777) == 0640);

    ::sleep(1);
    FDBFS_REQUIRE_OK(::truncate(p.c_str(), 2000));
    struct stat shrunk {};
    FDBFS_REQUIRE_OK(::stat(p.c_str(), &shrunk));
    CHECK(shrunk.st_size == 2000);
    CHECK(compare_timespec(shrunk.st_mtim, before.st_mtim) > 0);
    CHECK(compare_timespec(shrunk.st_ctim, before.st_ctim) > 0);

    ::sleep(1);
    FDBFS_REQUIRE_OK(::truncate(p.c_str(), 12000));
    struct stat grown {};
    FDBFS_REQUIRE_OK(::stat(p.c_str(), &grown));
    CHECK(grown.st_size == 12000);

    fd = ::open(p.c_str(), O_RDONLY);
    FDBFS_REQUIRE_NONNEG(fd);
    const auto whole = pread_exact_fd(fd, static_cast<size_t>(grown.st_size), 0);
    FDBFS_REQUIRE_OK(::close(fd));

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
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("utimens_file");
    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    struct timespec ts[2]{};
    ts[0].tv_sec = 1700000000;
    ts[0].tv_nsec = 123456789;
    ts[1].tv_sec = 1700000123;
    ts[1].tv_nsec = 987654321;

    FDBFS_REQUIRE_OK(::utimensat(AT_FDCWD, p.c_str(), ts, 0));

    struct stat st {};
    FDBFS_REQUIRE_OK(::stat(p.c_str(), &st));
    CHECK(st.st_atim.tv_sec == ts[0].tv_sec);
    CHECK(st.st_atim.tv_nsec == ts[0].tv_nsec);
    CHECK(st.st_mtim.tv_sec == ts[1].tv_sec);
    CHECK(st.st_mtim.tv_nsec == ts[1].tv_nsec);
  });
}

TEST_CASE("setattr through chown works when privileged",
          "[integration][setattr][stat]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("chown_file");
    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

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
    FDBFS_REQUIRE_OK(::stat(p.c_str(), &st));
    CHECK(st.st_uid == uid);
    CHECK(st.st_gid == gid);
  });
}

TEST_CASE("setattr substantial update clears setuid/setgid on regular files",
          "[integration][setattr][stat]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("suid_file");
    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 06755);
    FDBFS_REQUIRE_NONNEG(fd);
    const auto payload =
        generate_bytes(1024, BytePattern::Random, 0, 0xabcdefu);
    write_all_fd(fd, payload.data(), payload.size());
    FDBFS_REQUIRE_OK(::close(fd));

    FDBFS_REQUIRE_OK(::chmod(p.c_str(), 06755));
    struct stat before {};
    FDBFS_REQUIRE_OK(::stat(p.c_str(), &before));
    CHECK((before.st_mode & 06000) == 06000);

    FDBFS_REQUIRE_OK(::truncate(p.c_str(), 100));

    struct stat after {};
    FDBFS_REQUIRE_OK(::stat(p.c_str(), &after));
    CHECK(after.st_size == 100);
    CHECK((after.st_mode & 06000) == 0);
    CHECK((after.st_mode & 01777) == 0755);
  });
}

TEST_CASE("truncate on symlink path follows the target or fails for dangling",
          "[integration][setattr][symlink]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path target = env.p("truncate_target");
    const fs::path link = env.p("truncate_link");
    int fd = ::open(target.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    const char payload[] = "payload";
    write_all_fd(fd, reinterpret_cast<const uint8_t *>(payload),
                 sizeof(payload) - 1);
    FDBFS_REQUIRE_OK(::close(fd));

    FDBFS_REQUIRE_OK(::symlink(target.filename().c_str(), link.c_str()));

    struct stat target_before {};
    struct stat link_before {};
    FDBFS_REQUIRE_OK(::stat(target.c_str(), &target_before));
    FDBFS_REQUIRE_OK(::lstat(link.c_str(), &link_before));

    FDBFS_REQUIRE_OK(::truncate(link.c_str(), 1));

    struct stat target_after {};
    struct stat link_after {};
    FDBFS_REQUIRE_OK(::stat(target.c_str(), &target_after));
    FDBFS_REQUIRE_OK(::lstat(link.c_str(), &link_after));

    CHECK(target_after.st_size == 1);
    CHECK(link_after.st_size == link_before.st_size);

    const fs::path dangling = env.p("truncate_dangling");
    const std::string missing_target = "does-not-exist";
    FDBFS_REQUIRE_OK(::symlink(missing_target.c_str(), dangling.c_str()));

    struct stat dangling_before {};
    FDBFS_REQUIRE_OK(::lstat(dangling.c_str(), &dangling_before));

    errno = 0;
    CHECK(::truncate(dangling.c_str(), 1) == -1);
    CHECK(errno == ENOENT);

    struct stat dangling_after {};
    FDBFS_REQUIRE_OK(::lstat(dangling.c_str(), &dangling_after));
    CHECK(dangling_after.st_size == dangling_before.st_size);
  });
}
