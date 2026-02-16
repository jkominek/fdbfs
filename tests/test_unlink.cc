#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <string>

#include "test_posix_helpers.h"
#include "test_support.h"

TEST_CASE("unlink removes regular files and preserves hard-linked targets",
          "[integration][unlink][link][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path a = env.p("a");
    const fs::path b = env.p("b");

    int fd = ::open(a.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);
    REQUIRE(::link(a.c_str(), b.c_str()) == 0);

    REQUIRE(::unlink(a.c_str()) == 0);
    CHECK(!fs::exists(a));
    CHECK(fs::exists(b));

    struct stat st {};
    REQUIRE(::stat(b.c_str(), &st) == 0);
    CHECK(st.st_nlink == 1);

    REQUIRE(::unlink(b.c_str()) == 0);
    CHECK(!fs::exists(b));
  });
}

TEST_CASE("unlink and rmdir enforce directory/type constraints",
          "[integration][unlink][rmdir]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path dir = env.p("dir");
    const fs::path file = env.p("file");
    REQUIRE(::mkdir(dir.c_str(), 0755) == 0);

    int fd = ::open(file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    errno = 0;
    CHECK(::unlink(dir.c_str()) == -1);
    CHECK(errno == EISDIR);

    errno = 0;
    CHECK(::rmdir(file.c_str()) == -1);
    CHECK(errno == ENOTDIR);

    const fs::path nonempty = env.p("nonempty");
    REQUIRE(::mkdir(nonempty.c_str(), 0755) == 0);
    fd = ::open((nonempty / "child").c_str(), O_CREAT | O_WRONLY | O_TRUNC,
                0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    errno = 0;
    CHECK(::rmdir(nonempty.c_str()) == -1);
    CHECK(errno == ENOTEMPTY);

    REQUIRE(::unlink((nonempty / "child").c_str()) == 0);
    REQUIRE(::rmdir(nonempty.c_str()) == 0);

    errno = 0;
    CHECK(::unlink(env.p("missing").c_str()) == -1);
    CHECK(errno == ENOENT);
  });
}
