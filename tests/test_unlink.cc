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
  scenario([&](FdbfsEnv &env) {
    const fs::path a = env.p("a");
    const fs::path b = env.p("b");

    int fd = ::open(a.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));
    FDBFS_REQUIRE_OK(::link(a.c_str(), b.c_str()));

    FDBFS_REQUIRE_OK(::unlink(a.c_str()));
    CHECK(!fs::exists(a));
    CHECK(fs::exists(b));

    struct stat st {};
    FDBFS_REQUIRE_OK(::stat(b.c_str(), &st));
    CHECK(st.st_nlink == 1);

    FDBFS_REQUIRE_OK(::unlink(b.c_str()));
    CHECK(!fs::exists(b));
  });
}

TEST_CASE("unlink and rmdir enforce directory/type constraints",
          "[integration][unlink][rmdir]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path dir = env.p("dir");
    const fs::path file = env.p("file");
    FDBFS_REQUIRE_OK(::mkdir(dir.c_str(), 0755));

    int fd = ::open(file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    errno = 0;
    CHECK(::unlink(dir.c_str()) == -1);
    FDBFS_CHECK_ERRNO(EISDIR);

    errno = 0;
    CHECK(::rmdir(file.c_str()) == -1);
    FDBFS_CHECK_ERRNO(ENOTDIR);

    const fs::path nonempty = env.p("nonempty");
    FDBFS_REQUIRE_OK(::mkdir(nonempty.c_str(), 0755));
    fd = ::open((nonempty / "child").c_str(), O_CREAT | O_WRONLY | O_TRUNC,
                0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    errno = 0;
    CHECK(::rmdir(nonempty.c_str()) == -1);
    FDBFS_CHECK_ERRNO(ENOTEMPTY);

    FDBFS_REQUIRE_OK(::unlink((nonempty / "child").c_str()));
    FDBFS_REQUIRE_OK(::rmdir(nonempty.c_str()));

    errno = 0;
    CHECK(::unlink(env.p("missing").c_str()) == -1);
    FDBFS_CHECK_ERRNO(ENOENT);
  });
}
