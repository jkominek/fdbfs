#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <ctime>
#include <string>

#include "test_posix_helpers.h"
#include "test_support.h"

TEST_CASE("getattr reports expected metadata for major inode kinds",
          "[integration][getattr][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path reg = env.p("f");
    const fs::path dir = env.p("d");
    const fs::path sym = env.p("s");
    const std::string symlink_target = "target_path";

    int fd = ::open(reg.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0640);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);
    REQUIRE(::mkdir(dir.c_str(), 0750) == 0);
    REQUIRE(::symlink(symlink_target.c_str(), sym.c_str()) == 0);

    struct stat reg_st {};
    REQUIRE(::lstat(reg.c_str(), &reg_st) == 0);
    CHECK(S_ISREG(reg_st.st_mode));
    CHECK((reg_st.st_mode & 0777) == ((0640 & ~current_umask()) & 0777));
    CHECK(reg_st.st_nlink >= 1);

    struct stat dir_st {};
    REQUIRE(::lstat(dir.c_str(), &dir_st) == 0);
    CHECK(S_ISDIR(dir_st.st_mode));
    CHECK(dir_st.st_nlink >= 2);

    struct stat sym_st {};
    REQUIRE(::lstat(sym.c_str(), &sym_st) == 0);
    CHECK(S_ISLNK(sym_st.st_mode));
    CHECK(sym_st.st_size == static_cast<off_t>(symlink_target.size()));

    struct stat now_st {};
    struct timespec now {};
    REQUIRE(::clock_gettime(CLOCK_REALTIME, &now) == 0);
    REQUIRE(::lstat(reg.c_str(), &now_st) == 0);
    CHECK(is_recent(now_st.st_atim, now, 5));
    CHECK(is_recent(now_st.st_mtim, now, 5));
    CHECK(is_recent(now_st.st_ctim, now, 5));
  });
}

TEST_CASE("getattr missing path returns ENOENT", "[integration][getattr][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    errno = 0;
    struct stat st {};
    CHECK(::lstat(env.p("does_not_exist").c_str(), &st) == -1);
    CHECK(errno == ENOENT);
  });
}
