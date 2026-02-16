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

bool is_recent(const struct timespec &t, const struct timespec &now,
               time_t max_age_sec) {
  if (compare_timespec(t, now) > 0) {
    return false;
  }
  return (now.tv_sec - t.tv_sec) <= max_age_sec;
}

void require_stat_directory(const fs::path &p, struct stat &st) {
  REQUIRE(::stat(p.c_str(), &st) == 0);
  CHECK(S_ISDIR(st.st_mode));
}

} // namespace

TEST_CASE("mkdir creates a directory", "[integration][mkdir][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("d1");

    REQUIRE(::mkdir(p.c_str(), 0755) == 0);

    struct stat st{};
    require_stat_directory(p, st);
    const mode_t expected_mode = (0755 & ~current_umask()) & 0777;
    CHECK((st.st_mode & 0777) == expected_mode);
    CHECK(st.st_nlink >= 2);
  });
}

TEST_CASE("mkdir mode permutations are respected",
          "[integration][mkdir][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const mode_t um = current_umask();
    const std::vector<mode_t> requested_modes = {0000, 0001, 0010, 0100,
                                                 0555, 0700, 0755, 0777};

    for (size_t i = 0; i < requested_modes.size(); i++) {
      const fs::path p = env.p("perm_" + std::to_string(i));
      REQUIRE(::mkdir(p.c_str(), requested_modes[i]) == 0);

      struct stat st{};
      require_stat_directory(p, st);

      const mode_t expected_mode = (requested_modes[i] & ~um) & 0777;
      CHECK((st.st_mode & 0777) == expected_mode);
    }
  });
}

TEST_CASE("mkdir existing directory returns EEXIST", "[integration][mkdir]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("dupe");
    REQUIRE(::mkdir(p.c_str(), 0755) == 0);

    errno = 0;
    CHECK(::mkdir(p.c_str(), 0755) == -1);
    CHECK(errno == EEXIST);
  });
}

TEST_CASE("mkdir with missing parent returns ENOENT", "[integration][mkdir]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("missing_parent/child");

    errno = 0;
    CHECK(::mkdir(p.c_str(), 0755) == -1);
    CHECK(errno == ENOENT);
  });
}

TEST_CASE("mkdir existing file returns EEXIST", "[integration][mkdir][open]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("occupied");
    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    errno = 0;
    CHECK(::mkdir(p.c_str(), 0755) == -1);
    CHECK(errno == EEXIST);
  });
}

TEST_CASE("mkdir long name returns ENAMETOOLONG", "[integration][mkdir]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const std::string max_name(255, 'a');
    const std::string too_long_name(256, 'a');

    const fs::path max_path = env.p(max_name);
    REQUIRE(::mkdir(max_path.c_str(), 0755) == 0);
    struct stat st{};
    require_stat_directory(max_path, st);

    const fs::path too_long_path = env.p(too_long_name);
    errno = 0;
    CHECK(::mkdir(too_long_path.c_str(), 0755) == -1);
    CHECK(errno == ENAMETOOLONG);
  });
}

TEST_CASE("mkdir deep nested chain succeeds", "[integration][mkdir][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    fs::path p = env.mnt;
    for (int i = 0; i < 32; i++) {
      p /= ("d" + std::to_string(i));
      REQUIRE(::mkdir(p.c_str(), 0755) == 0);
      struct stat st{};
      require_stat_directory(p, st);
    }
  });
}

TEST_CASE("mkdir increases parent directory nlink for subdirectories",
          "[integration][mkdir][stat][!shouldfail]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path parent = env.p("nlink_parent");
    REQUIRE(::mkdir(parent.c_str(), 0755) == 0);

    struct stat pst{};
    require_stat_directory(parent, pst);
    const nlink_t base_nlink = pst.st_nlink;

    for (int i = 0; i < 4; i++) {
      const fs::path child = parent / ("child_" + std::to_string(i));
      REQUIRE(::mkdir(child.c_str(), 0755) == 0);

      REQUIRE(::stat(parent.c_str(), &pst) == 0);
      CHECK(pst.st_nlink == static_cast<nlink_t>(base_nlink + i + 1));
    }
  });
}

TEST_CASE("mkdir updates parent ctime/mtime and sets recent times",
          "[integration][mkdir][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path parent = env.p("timed");
    REQUIRE(::mkdir(parent.c_str(), 0755) == 0);

    struct stat pst{};
    require_stat_directory(parent, pst);

    struct timespec now{};
    REQUIRE(::clock_gettime(CLOCK_REALTIME, &now) == 0);
    CHECK(is_recent(pst.st_atim, now, 5));
    CHECK(is_recent(pst.st_mtim, now, 5));
    CHECK(is_recent(pst.st_ctim, now, 5));

    const struct timespec old_mtim = pst.st_mtim;
    const struct timespec old_ctim = pst.st_ctim;

    ::sleep(1);
    const fs::path child = parent / "child";
    REQUIRE(::mkdir(child.c_str(), 0755) == 0);
    struct stat cst{};
    require_stat_directory(child, cst);

    REQUIRE(::stat(parent.c_str(), &pst) == 0);
    CHECK(compare_timespec(pst.st_mtim, old_mtim) > 0);
    CHECK(compare_timespec(pst.st_ctim, old_ctim) > 0);
  });
}

TEST_CASE("mkdir path through regular file returns ENOTDIR",
          "[integration][mkdir][open]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path dir = env.p("container");
    REQUIRE(::mkdir(dir.c_str(), 0755) == 0);

    const fs::path file = dir / "file";
    int fd = ::open(file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    const fs::path invalid_subdir = file / "child";
    errno = 0;
    CHECK(::mkdir(invalid_subdir.c_str(), 0755) == -1);
    CHECK(errno == ENOTDIR);
  });
}
