#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <string>
#include <vector>

#include "test_support.h"

TEST_CASE("readlink returns exact symlink target", "[integration][readlink]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path link_path = env.p("sym");
    const std::string target = "some/target/path";

    REQUIRE(::symlink(target.c_str(), link_path.c_str()) == 0);

    std::vector<char> out(target.size() + 16, '\0');
    const ssize_t n = ::readlink(link_path.c_str(), out.data(), out.size());
    REQUIRE(n >= 0);
    out.resize(static_cast<size_t>(n));
    CHECK(std::string(out.begin(), out.end()) == target);
  });
}

TEST_CASE("readlink errors on non-symlink and missing path",
          "[integration][readlink]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path regular = env.p("regular");
    int fd = ::open(regular.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    std::vector<char> out(64, '\0');

    errno = 0;
    CHECK(::readlink(regular.c_str(), out.data(), out.size()) == -1);
    CHECK(errno == EINVAL);

    errno = 0;
    CHECK(::readlink(env.p("missing").c_str(), out.data(), out.size()) == -1);
    CHECK(errno == ENOENT);
  });
}

TEST_CASE("symlink rejects target longer than internal limit",
          "[integration][readlink][mknod]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const std::string long_target(1025, 't');
    errno = 0;
    CHECK(::symlink(long_target.c_str(), env.p("too_long_target").c_str()) ==
          -1);
    CHECK(errno == ENAMETOOLONG);
  });
}
