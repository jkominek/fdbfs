#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>

#include "test_posix_helpers.h"
#include "test_support.h"

namespace {

int renameat2_wrapper(const fs::path &from, const fs::path &to,
                      unsigned flags) {
#ifdef SYS_renameat2
  return static_cast<int>(::syscall(SYS_renameat2, AT_FDCWD, from.c_str(),
                                    AT_FDCWD, to.c_str(), flags));
#else
  (void)from;
  (void)to;
  (void)flags;
  errno = ENOSYS;
  return -1;
#endif
}

void create_file_with_contents(const fs::path &p, std::string_view s) {
  int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  REQUIRE(fd >= 0);
  REQUIRE(::write(fd, s.data(), s.size()) == static_cast<ssize_t>(s.size()));
  REQUIRE(::close(fd) == 0);
}

} // namespace

TEST_CASE("rename basic and replacement behavior", "[integration][rename]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path a = env.p("a");
    const fs::path b = env.p("b");
    create_file_with_contents(a, "aaa");

    REQUIRE(::rename(a.c_str(), b.c_str()) == 0);
    CHECK(!fs::exists(a));
    CHECK(fs::exists(b));

    const fs::path c = env.p("c");
    create_file_with_contents(c, "ccc");

    REQUIRE(::rename(b.c_str(), c.c_str()) == 0);
    CHECK(!fs::exists(b));
    CHECK(fs::exists(c));

    const auto data = read_file_all(c);
    CHECK(std::string(data.begin(), data.end()) == "aaa");
  });
}

TEST_CASE("rename type and emptiness checks", "[integration][rename]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path file1 = env.p("f1");
    const fs::path file2 = env.p("f2");
    const fs::path dir1 = env.p("d1");
    const fs::path dir2 = env.p("d2");
    create_file_with_contents(file1, "x");
    create_file_with_contents(file2, "y");
    REQUIRE(::mkdir(dir1.c_str(), 0755) == 0);
    REQUIRE(::mkdir(dir2.c_str(), 0755) == 0);
    create_file_with_contents(dir2 / "child", "z");

    errno = 0;
    CHECK(::rename(file1.c_str(), dir1.c_str()) == -1);
    CHECK(errno == EISDIR);

    errno = 0;
    CHECK(::rename(dir1.c_str(), file2.c_str()) == -1);
    CHECK(errno == ENOTDIR);

    errno = 0;
    CHECK(::rename(dir1.c_str(), dir2.c_str()) == -1);
    CHECK(errno == ENOTEMPTY);
  });
}

TEST_CASE("rename directory onto empty directory succeeds",
          "[integration][rename][mkdir]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path src = env.p("srcdir");
    const fs::path dst = env.p("dstdir");
    REQUIRE(::mkdir(src.c_str(), 0755) == 0);
    REQUIRE(::mkdir(dst.c_str(), 0755) == 0);

    REQUIRE(::rename(src.c_str(), dst.c_str()) == 0);
    CHECK(!fs::exists(src));
    CHECK(fs::exists(dst));
    CHECK(fs::is_directory(dst));
  });
}

TEST_CASE("renameat2 NOREPLACE and EXCHANGE", "[integration][rename]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path left = env.p("left");
    const fs::path right = env.p("right");
    create_file_with_contents(left, "L");
    create_file_with_contents(right, "R");

    errno = 0;
    if (renameat2_wrapper(left, right, RENAME_NOREPLACE) == -1) {
      if (errno == ENOSYS) {
        SKIP("renameat2 not available on this platform/kernel");
      }
      CHECK(errno == EEXIST);
    } else {
      FAIL("renameat2(RENAME_NOREPLACE) unexpectedly succeeded");
    }

    REQUIRE(renameat2_wrapper(left, right, RENAME_EXCHANGE) == 0);
    const auto left_data = read_file_all(left);
    const auto right_data = read_file_all(right);
    CHECK(std::string(left_data.begin(), left_data.end()) == "R");
    CHECK(std::string(right_data.begin(), right_data.end()) == "L");

#ifdef RENAME_WHITEOUT
    errno = 0;
    CHECK(renameat2_wrapper(left, right, RENAME_WHITEOUT) == -1);
    CHECK((errno == ENOSYS || errno == EINVAL));
#endif
  });
}
