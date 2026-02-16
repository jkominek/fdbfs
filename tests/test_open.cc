#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <ctime>
#include <string>
#include <vector>

#include "test_support.h"

namespace {

mode_t current_umask() {
  const mode_t old_umask = ::umask(0);
  ::umask(old_umask);
  return old_umask;
}

void require_stat_regular_file(const fs::path &p, struct stat &st) {
  REQUIRE(::stat(p.c_str(), &st) == 0);
  CHECK(S_ISREG(st.st_mode));
}

int compare_timespec(const struct timespec &a, const struct timespec &b) {
  if (a.tv_sec < b.tv_sec) {
    return -1;
  }
  if (a.tv_sec > b.tv_sec) {
    return 1;
  }
  if (a.tv_nsec < b.tv_nsec) {
    return -1;
  }
  if (a.tv_nsec > b.tv_nsec) {
    return 1;
  }
  return 0;
}

} // namespace

TEST_CASE("open existing files read-only succeeds across many files",
          "[integration][open][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    for (int i = 0; i < 256; i++) {
      const fs::path p = env.p("existing_" + std::to_string(i));

      int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
      REQUIRE(fd >= 0);
      REQUIRE(::close(fd) == 0);

      fd = ::open(p.c_str(), O_RDONLY);
      REQUIRE(fd >= 0);
      REQUIRE(::close(fd) == 0);

      struct stat st{};
      require_stat_regular_file(p, st);
    }
  });
}

TEST_CASE("open missing file without O_CREAT returns ENOENT",
          "[integration][open]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("missing");
    errno = 0;
    CHECK(::open(p.c_str(), O_RDONLY) == -1);
    CHECK(errno == ENOENT);
  });
}

TEST_CASE("open O_CREAT respects requested modes",
          "[integration][open][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const mode_t um = current_umask();
    const std::vector<mode_t> requested_modes = {0000, 0001, 0010, 0100,
                                                 0555, 0700, 0751, 0777};
    for (size_t i = 0; i < requested_modes.size(); i++) {
      const fs::path p = env.p("mode_" + std::to_string(i));
      const mode_t expected_mode = (requested_modes[i] & ~um) & 0777;
      int fd =
          ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, requested_modes[i]);
      REQUIRE(fd >= 0);
      REQUIRE(::close(fd) == 0);

      struct stat st{};
      require_stat_regular_file(p, st);
      CHECK((st.st_mode & 0777) == expected_mode);
    }
  });
}

TEST_CASE("open O_CREAT O_EXCL on existing file returns EEXIST",
          "[integration][open]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("dupe");
    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    errno = 0;
    CHECK(::open(p.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644) == -1);
    CHECK(errno == EEXIST);
  });
}

TEST_CASE("open O_CREAT updates parent ctime/mtime",
          "[integration][open][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path parent = env.p("parent_create");
    REQUIRE(::mkdir(parent.c_str(), 0755) == 0);

    struct stat pst_before{};
    REQUIRE(::stat(parent.c_str(), &pst_before) == 0);
    ::sleep(1);

    const fs::path child = parent / "newfile";
    int fd = ::open(child.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    struct stat pst_after{};
    REQUIRE(::stat(parent.c_str(), &pst_after) == 0);
    CHECK(compare_timespec(pst_after.st_mtim, pst_before.st_mtim) > 0);
    CHECK(compare_timespec(pst_after.st_ctim, pst_before.st_ctim) > 0);
  });
}

TEST_CASE("open existing file does not update parent ctime/mtime",
          "[integration][open][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path parent = env.p("parent_existing");
    REQUIRE(::mkdir(parent.c_str(), 0755) == 0);

    const fs::path child = parent / "existing";
    int fd = ::open(child.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    struct stat pst_before{};
    REQUIRE(::stat(parent.c_str(), &pst_before) == 0);
    ::sleep(1);

    fd = ::open(child.c_str(), O_RDONLY);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    struct stat pst_after{};
    REQUIRE(::stat(parent.c_str(), &pst_after) == 0);
    CHECK(compare_timespec(pst_after.st_mtim, pst_before.st_mtim) == 0);
    CHECK(compare_timespec(pst_after.st_ctim, pst_before.st_ctim) == 0);
  });
}

TEST_CASE("open O_TRUNC truncates non-empty file and updates file times",
          "[integration][open][write][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("truncate_me");
    const std::string payload = "this will be truncated";

    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::write(fd, payload.data(), payload.size()) ==
            static_cast<ssize_t>(payload.size()));
    REQUIRE(::close(fd) == 0);

    struct stat st{};
    require_stat_regular_file(p, st);
    CHECK(st.st_size == static_cast<off_t>(payload.size()));
    const struct timespec old_mtim = st.st_mtim;
    const struct timespec old_ctim = st.st_ctim;

    ::sleep(1);
    fd = ::open(p.c_str(), O_WRONLY | O_TRUNC);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    REQUIRE(::stat(p.c_str(), &st) == 0);
    CHECK(st.st_size == 0);
    CHECK(compare_timespec(st.st_mtim, old_mtim) > 0);
    CHECK(compare_timespec(st.st_ctim, old_ctim) > 0);
  });
}

TEST_CASE("open O_TRUNC on already-empty file keeps size and file times",
          "[integration][open][write][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("already_empty");

    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    struct stat st_before{};
    require_stat_regular_file(p, st_before);
    CHECK(st_before.st_size == 0);

    ::sleep(1);
    fd = ::open(p.c_str(), O_WRONLY | O_TRUNC);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    struct stat st_after{};
    require_stat_regular_file(p, st_after);
    CHECK(st_after.st_size == 0);
    CHECK(compare_timespec(st_after.st_mtim, st_before.st_mtim) == 0);
    CHECK(compare_timespec(st_after.st_ctim, st_before.st_ctim) == 0);
  });
}

TEST_CASE("open through existing file path component returns ENOTDIR",
          "[integration][open]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path file = env.p("foo");
    int fd = ::open(file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    const fs::path invalid = env.p("foo/bar");
    errno = 0;
    CHECK(::open(invalid.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644) == -1);
    CHECK(errno == ENOTDIR);
  });
}

TEST_CASE("open through missing parent returns ENOENT", "[integration][open]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path missing_parent = env.p("doesnotexist/name");
    errno = 0;
    CHECK(::open(missing_parent.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644) ==
          -1);
    CHECK(errno == ENOENT);
  });
}

TEST_CASE("open too-long filename returns ENAMETOOLONG",
          "[integration][open]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const std::string max_name(255, 'b');
    const std::string too_long_name(256, 'b');

    int fd =
        ::open(env.p(max_name).c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    errno = 0;
    CHECK(::open(env.p(too_long_name).c_str(), O_CREAT | O_WRONLY | O_TRUNC,
                 0644) == -1);
    CHECK(errno == ENAMETOOLONG);
  });
}

TEST_CASE("open directory for writing fails", "[integration][open][mkdir]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path dir = env.p("dir_write_fail");
    REQUIRE(::mkdir(dir.c_str(), 0755) == 0);

    errno = 0;
    CHECK(::open(dir.c_str(), O_WRONLY) == -1);
    CHECK(errno == EISDIR);
  });
}
