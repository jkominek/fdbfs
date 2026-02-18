#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <string>

#include "test_posix_helpers.h"
#include "test_support.h"

TEST_CASE("link creates hard link to regular file and shares inode",
          "[integration][link][unlink][stat]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path original = env.p("orig");
    const fs::path linked = env.p("linked");
    const std::string payload = "hard-link-data";

    int fd = ::open(original.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    REQUIRE(::write(fd, payload.data(), payload.size()) ==
            static_cast<ssize_t>(payload.size()));
    FDBFS_REQUIRE_OK(::close(fd));

    FDBFS_REQUIRE_OK(::link(original.c_str(), linked.c_str()));

    struct stat st1 {};
    struct stat st2 {};
    FDBFS_REQUIRE_OK(::stat(original.c_str(), &st1));
    FDBFS_REQUIRE_OK(::stat(linked.c_str(), &st2));
    CHECK(st1.st_ino == st2.st_ino);
    CHECK(st1.st_nlink == 2);
    CHECK(st2.st_nlink == 2);

    const auto read_back = read_file_all(linked);
    CHECK(std::string(read_back.begin(), read_back.end()) == payload);

    FDBFS_REQUIRE_OK(::unlink(original.c_str()));
    FDBFS_REQUIRE_OK(::stat(linked.c_str(), &st2));
    CHECK(st2.st_nlink == 1);
  });
}

TEST_CASE("link errors for invalid targets and destinations",
          "[integration][link]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path f = env.p("f");
    const fs::path d = env.p("d");
    int fd = ::open(f.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));
    FDBFS_REQUIRE_OK(::mkdir(d.c_str(), 0755));

    errno = 0;
    CHECK(::link(d.c_str(), env.p("link_to_dir").c_str()) == -1);
    FDBFS_CHECK_ERRNO(EPERM);

    errno = 0;
    CHECK(::link(f.c_str(), env.p("missing_parent/child").c_str()) == -1);
    FDBFS_CHECK_ERRNO(ENOENT);

    errno = 0;
    CHECK(::link(env.p("missing_src").c_str(), env.p("dest").c_str()) == -1);
    FDBFS_CHECK_ERRNO(ENOENT);

    errno = 0;
    CHECK(::link(f.c_str(), f.c_str()) == -1);
    FDBFS_CHECK_ERRNO(EEXIST);

    errno = 0;
    CHECK(::link(f.c_str(), env.p("f/child").c_str()) == -1);
    FDBFS_CHECK_ERRNO(ENOTDIR);
  });
}
