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
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path original = env.p("orig");
    const fs::path linked = env.p("linked");
    const std::string payload = "hard-link-data";

    int fd = ::open(original.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::write(fd, payload.data(), payload.size()) ==
            static_cast<ssize_t>(payload.size()));
    REQUIRE(::close(fd) == 0);

    REQUIRE(::link(original.c_str(), linked.c_str()) == 0);

    struct stat st1 {};
    struct stat st2 {};
    REQUIRE(::stat(original.c_str(), &st1) == 0);
    REQUIRE(::stat(linked.c_str(), &st2) == 0);
    CHECK(st1.st_ino == st2.st_ino);
    CHECK(st1.st_nlink == 2);
    CHECK(st2.st_nlink == 2);

    const auto read_back = read_file_all(linked);
    CHECK(std::string(read_back.begin(), read_back.end()) == payload);

    REQUIRE(::unlink(original.c_str()) == 0);
    REQUIRE(::stat(linked.c_str(), &st2) == 0);
    CHECK(st2.st_nlink == 1);
  });
}

TEST_CASE("link errors for invalid targets and destinations",
          "[integration][link]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path f = env.p("f");
    const fs::path d = env.p("d");
    int fd = ::open(f.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);
    REQUIRE(::mkdir(d.c_str(), 0755) == 0);

    errno = 0;
    CHECK(::link(d.c_str(), env.p("link_to_dir").c_str()) == -1);
    CHECK(errno == EPERM);

    errno = 0;
    CHECK(::link(f.c_str(), env.p("missing_parent/child").c_str()) == -1);
    CHECK(errno == ENOENT);

    errno = 0;
    CHECK(::link(env.p("missing_src").c_str(), env.p("dest").c_str()) == -1);
    CHECK(errno == ENOENT);

    errno = 0;
    CHECK(::link(f.c_str(), f.c_str()) == -1);
    CHECK(errno == EEXIST);

    errno = 0;
    CHECK(::link(f.c_str(), env.p("f/child").c_str()) == -1);
    CHECK(errno == ENOTDIR);
  });
}
