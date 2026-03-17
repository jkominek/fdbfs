#include <catch2/catch_test_macros.hpp>

#include <array>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <ctime>
#include <string>

#include "test_posix_helpers.h"
#include "test_support.h"

namespace {

struct TimestampExpectations {
  bool atime_changed;
  bool mtime_changed;
  bool ctime_changed;
};

struct stat require_stat_snapshot(const fs::path &p) {
  struct stat st{};
  FDBFS_REQUIRE_OK(::stat(p.c_str(), &st));
  return st;
}

void sleep_for_timestamp_tick() { ::sleep(1); }

void check_time_transition(std::string_view label, const struct stat &before,
                           const struct stat &after,
                           TimestampExpectations expectations) {
  INFO("transition=" << label);

  const auto atime_cmp = compare_timespec(after.st_atim, before.st_atim);
  const auto mtime_cmp = compare_timespec(after.st_mtim, before.st_mtim);
  const auto ctime_cmp = compare_timespec(after.st_ctim, before.st_ctim);

  INFO("atime before=" << before.st_atim.tv_sec << "." << before.st_atim.tv_nsec
                       << " after=" << after.st_atim.tv_sec << "."
                       << after.st_atim.tv_nsec << " cmp=" << atime_cmp);
  INFO("mtime before=" << before.st_mtim.tv_sec << "." << before.st_mtim.tv_nsec
                       << " after=" << after.st_mtim.tv_sec << "."
                       << after.st_mtim.tv_nsec << " cmp=" << mtime_cmp);
  INFO("ctime before=" << before.st_ctim.tv_sec << "." << before.st_ctim.tv_nsec
                       << " after=" << after.st_ctim.tv_sec << "."
                       << after.st_ctim.tv_nsec << " cmp=" << ctime_cmp);

  if (expectations.atime_changed) {
    CHECK(atime_cmp > 0);
  } else {
    CHECK(atime_cmp == 0);
  }

  if (expectations.mtime_changed) {
    CHECK(mtime_cmp > 0);
  } else {
    CHECK(mtime_cmp == 0);
  }

  if (expectations.ctime_changed) {
    CHECK(ctime_cmp > 0);
  } else {
    CHECK(ctime_cmp == 0);
  }
}

void read_single_byte(const fs::path &p) {
  int fd = ::open(p.c_str(), O_RDONLY);
  FDBFS_REQUIRE_NONNEG(fd);
  char c = '\0';
  const ssize_t n = ::read(fd, &c, 1);
  FDBFS_REQUIRE_NONNEG(n);
  CHECK(n == 1);
  FDBFS_REQUIRE_OK(::close(fd));
}

void write_single_byte(const fs::path &p, off_t off, uint8_t value) {
  int fd = ::open(p.c_str(), O_WRONLY);
  FDBFS_REQUIRE_NONNEG(fd);
  const ssize_t n = ::pwrite(fd, &value, 1, off);
  FDBFS_REQUIRE_NONNEG(n);
  CHECK(n == 1);
  FDBFS_REQUIRE_OK(::close(fd));
}

void run_timestamp_behavior_matrix(FdbfsEnv &env) {
  SECTION("regular file inode timestamps") {
    const fs::path p = env.p("ts-file");
    const fs::path alias = env.p("ts-file-link");

    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    const std::array<uint8_t, 4> seed = {'a', 'b', 'c', 'd'};
    write_all_fd(fd, seed.data(), seed.size());
    FDBFS_REQUIRE_OK(::close(fd));

    struct stat before = require_stat_snapshot(p);

    sleep_for_timestamp_tick();
    read_single_byte(p);
    struct stat after_read = require_stat_snapshot(p);
    check_time_transition("read", before, after_read,
                          TimestampExpectations{
                              .atime_changed = true,
                              .mtime_changed = false,
                              .ctime_changed = false,
                          });

    sleep_for_timestamp_tick();
    write_single_byte(p, 1, 'Z');
    struct stat after_write = require_stat_snapshot(p);
    check_time_transition("write", after_read, after_write,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });

    sleep_for_timestamp_tick();
    FDBFS_REQUIRE_OK(::chmod(p.c_str(), 0600));
    struct stat after_chmod = require_stat_snapshot(p);
    check_time_transition("chmod", after_write, after_chmod,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = false,
                              .ctime_changed = true,
                          });

    sleep_for_timestamp_tick();
    FDBFS_REQUIRE_OK(::link(p.c_str(), alias.c_str()));
    struct stat after_link = require_stat_snapshot(p);
    struct stat alias_after_link = require_stat_snapshot(alias);
    check_time_transition("link", after_chmod, after_link,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = false,
                              .ctime_changed = true,
                          });
    CHECK(after_link.st_ino == alias_after_link.st_ino);
    CHECK(after_link.st_nlink == 2);
    CHECK(alias_after_link.st_nlink == 2);

    sleep_for_timestamp_tick();
    FDBFS_REQUIRE_OK(::unlink(p.c_str()));
    struct stat after_unlink = require_stat_snapshot(alias);
    check_time_transition("unlink", after_link, after_unlink,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = false,
                              .ctime_changed = true,
                          });
    CHECK(after_unlink.st_nlink == 1);

    sleep_for_timestamp_tick();
    FDBFS_REQUIRE_OK(::truncate(alias.c_str(), 1));
    struct stat after_truncate = require_stat_snapshot(alias);
    check_time_transition("truncate", after_unlink, after_truncate,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });
    CHECK(after_truncate.st_size == 1);
  }

  SECTION("parent directory timestamps") {
    const fs::path dir = env.p("ts-dir");
    const fs::path child_a = dir / "child-a";
    const fs::path child_b = dir / "child-b";

    FDBFS_REQUIRE_OK(::mkdir(dir.c_str(), 0755));
    struct stat before = require_stat_snapshot(dir);

    sleep_for_timestamp_tick();
    int fd = ::open(child_a.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));
    struct stat after_create = require_stat_snapshot(dir);
    check_time_transition("create child", before, after_create,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });

    sleep_for_timestamp_tick();
    FDBFS_REQUIRE_OK(::rename(child_a.c_str(), child_b.c_str()));
    struct stat after_rename = require_stat_snapshot(dir);
    check_time_transition("rename child", after_create, after_rename,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });

    sleep_for_timestamp_tick();
    FDBFS_REQUIRE_OK(::unlink(child_b.c_str()));
    struct stat after_unlink = require_stat_snapshot(dir);
    check_time_transition("unlink child", after_rename, after_unlink,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });
  }
}

} // namespace

TEST_CASE("getattr reports expected metadata for major inode kinds",
          "[integration][getattr][stat]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path reg = env.p("f");
    const fs::path dir = env.p("d");
    const fs::path sym = env.p("s");
    const std::string symlink_target = "target_path";

    int fd = ::open(reg.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0640);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));
    FDBFS_REQUIRE_OK(::mkdir(dir.c_str(), 0750));
    FDBFS_REQUIRE_OK(::symlink(symlink_target.c_str(), sym.c_str()));

    struct stat reg_st{};
    FDBFS_REQUIRE_OK(::lstat(reg.c_str(), &reg_st));
    CHECK(S_ISREG(reg_st.st_mode));
    CHECK((reg_st.st_mode & 0777) == ((0640 & ~current_umask()) & 0777));
    CHECK(reg_st.st_nlink >= 1);

    struct stat dir_st{};
    FDBFS_REQUIRE_OK(::lstat(dir.c_str(), &dir_st));
    CHECK(S_ISDIR(dir_st.st_mode));
    CHECK(dir_st.st_nlink >= 2);

    struct stat sym_st{};
    FDBFS_REQUIRE_OK(::lstat(sym.c_str(), &sym_st));
    CHECK(S_ISLNK(sym_st.st_mode));
    CHECK(sym_st.st_size == static_cast<off_t>(symlink_target.size()));

    struct stat now_st{};
    struct timespec now{};
    FDBFS_REQUIRE_OK(::clock_gettime(CLOCK_REALTIME, &now));
    FDBFS_REQUIRE_OK(::lstat(reg.c_str(), &now_st));
    CHECK(is_recent(now_st.st_atim, now, 5));
    CHECK(is_recent(now_st.st_mtim, now, 5));
    CHECK(is_recent(now_st.st_ctim, now, 5));
  });
}

TEST_CASE("getattr missing path returns ENOENT",
          "[integration][getattr][stat]") {
  scenario([&](FdbfsEnv &env) {
    errno = 0;
    struct stat st{};
    CHECK(::lstat(env.p("does_not_exist").c_str(), &st) == -1);
    FDBFS_CHECK_ERRNO(ENOENT);
  });
}

TEST_CASE("getattr timestamp behavior matrix for files and directories on host "
          "backend",
          "[integration][getattr][stat][timestamps]") {
  scenario([&](FdbfsEnv &env) {
    if (!is_host_backend()) {
      SKIP("host backend has the passing variant of this test");
    }
    run_timestamp_behavior_matrix(env);
  });
}

// Obviously, this _shouldn't_ fail, but at the moment we expect it to,
// because we need to fix the timestamp behavior.
TEST_CASE("getattr timestamp behavior matrix for files and directories",
          "[integration][getattr][stat][timestamps][!shouldfail]") {
  scenario([&](FdbfsEnv &env) {
    if (is_host_backend()) {
      SKIP("host backend has the passing variant of this test");
    }
    run_timestamp_behavior_matrix(env);
  });
}
