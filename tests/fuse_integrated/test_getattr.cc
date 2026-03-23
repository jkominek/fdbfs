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

void sleep_for_attr_cache_expiry() {
  struct timespec ts{
      .tv_sec = 0,
      .tv_nsec = 50 * 1000 * 1000,
  };
  FDBFS_REQUIRE_OK(::nanosleep(&ts, nullptr));
}

void check_single_time_transition(std::string_view name,
                                  const struct timespec &before,
                                  const struct timespec &immediate,
                                  const struct timespec &after,
                                  bool expected_change) {
  const auto final_cmp = compare_timespec(after, before);
  const auto immediate_vs_before = compare_timespec(immediate, before);
  const auto immediate_vs_after = compare_timespec(immediate, after);

  INFO(name << " before=" << before.tv_sec << "." << before.tv_nsec
            << " immediate=" << immediate.tv_sec << "." << immediate.tv_nsec
            << " after=" << after.tv_sec << "." << after.tv_nsec
            << " final_cmp=" << final_cmp
            << " immediate_vs_before=" << immediate_vs_before
            << " immediate_vs_after=" << immediate_vs_after);

  if (expected_change) {
    CHECK(final_cmp > 0);
  } else {
    CHECK(final_cmp == 0);
  }

  CHECK(((immediate_vs_before == 0) || (immediate_vs_after == 0)));
}

void check_time_transition(std::string_view label, const struct stat &before,
                           const struct stat &immediate,
                           const struct stat &after,
                           TimestampExpectations expectations) {
  INFO("transition=" << label);

  check_single_time_transition("atime", before.st_atim, immediate.st_atim,
                               after.st_atim, expectations.atime_changed);
  check_single_time_transition("mtime", before.st_mtim, immediate.st_mtim,
                               after.st_mtim, expectations.mtime_changed);
  check_single_time_transition("ctime", before.st_ctim, immediate.st_ctim,
                               after.st_ctim, expectations.ctime_changed);
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

void set_only_atime(const fs::path &p, const struct timespec &atime) {
  struct timespec times[2]{};
  times[0] = atime;
  times[1].tv_nsec = UTIME_OMIT;
  FDBFS_REQUIRE_OK(::utimensat(AT_FDCWD, p.c_str(), times, 0));
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
    struct stat after_read_immediate = require_stat_snapshot(p);
    sleep_for_attr_cache_expiry();
    struct stat after_read = require_stat_snapshot(p);
    check_time_transition("read", before, after_read_immediate, after_read,
                          TimestampExpectations{
                              .atime_changed = true,
                              .mtime_changed = false,
                              .ctime_changed = false,
                          });

    sleep_for_timestamp_tick();
    write_single_byte(p, 1, 'Z');
    struct stat after_write_immediate = require_stat_snapshot(p);
    sleep_for_attr_cache_expiry();
    struct stat after_write = require_stat_snapshot(p);
    check_time_transition("write", after_read, after_write_immediate,
                          after_write,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });

    sleep_for_timestamp_tick();
    FDBFS_REQUIRE_OK(::chmod(p.c_str(), 0600));
    struct stat after_chmod_immediate = require_stat_snapshot(p);
    sleep_for_attr_cache_expiry();
    struct stat after_chmod = require_stat_snapshot(p);
    check_time_transition("chmod", after_write, after_chmod_immediate,
                          after_chmod,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = false,
                              .ctime_changed = true,
                          });

    sleep_for_timestamp_tick();
    FDBFS_REQUIRE_OK(::link(p.c_str(), alias.c_str()));
    struct stat after_link_immediate = require_stat_snapshot(p);
    sleep_for_attr_cache_expiry();
    struct stat after_link = require_stat_snapshot(p);
    struct stat alias_after_link = require_stat_snapshot(alias);
    check_time_transition("link", after_chmod, after_link_immediate, after_link,
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
    struct stat after_unlink_immediate = require_stat_snapshot(alias);
    sleep_for_attr_cache_expiry();
    struct stat after_unlink = require_stat_snapshot(alias);
    check_time_transition("unlink", after_link, after_unlink_immediate,
                          after_unlink,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = false,
                              .ctime_changed = true,
                          });
    CHECK(after_unlink.st_nlink == 1);

    sleep_for_timestamp_tick();
    FDBFS_REQUIRE_OK(::truncate(alias.c_str(), 1));
    struct stat after_truncate_immediate = require_stat_snapshot(alias);
    sleep_for_attr_cache_expiry();
    struct stat after_truncate = require_stat_snapshot(alias);
    check_time_transition("truncate", after_unlink, after_truncate_immediate,
                          after_truncate,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });
    CHECK(after_truncate.st_size == 1);
  }

  SECTION("parent directory timestamps") {
    const fs::path dir = env.p("ts-dir");
    const fs::path dummy = dir / "dummy";
    const fs::path child_a = dir / "child-a";
    const fs::path child_b = dir / "child-b";

    FDBFS_REQUIRE_OK(::mkdir(dir.c_str(), 0755));
    ::close(::open(dummy.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644));

    sleep_for_timestamp_tick();
    struct stat before_create = require_stat_snapshot(dir);
    int fd = ::open(child_a.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));
    struct stat after_create_immediate = require_stat_snapshot(dir);
    sleep_for_attr_cache_expiry();
    struct stat after_create = require_stat_snapshot(dir);
    check_time_transition("create child", before_create, after_create_immediate,
                          after_create,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });

    sleep_for_timestamp_tick();
    struct stat before_rename = require_stat_snapshot(dir);
    FDBFS_REQUIRE_OK(::rename(child_a.c_str(), child_b.c_str()));
    struct stat after_rename_immediate = require_stat_snapshot(dir);
    sleep_for_attr_cache_expiry();
    struct stat after_rename = require_stat_snapshot(dir);
    check_time_transition("rename child", before_rename, after_rename_immediate,
                          after_rename,
                          TimestampExpectations{
                              .atime_changed = false,
                              .mtime_changed = true,
                              .ctime_changed = true,
                          });

    sleep_for_timestamp_tick();
    struct stat before_unlink = require_stat_snapshot(dir);
    FDBFS_REQUIRE_OK(::unlink(child_b.c_str()));
    struct stat after_unlink_immediate = require_stat_snapshot(dir);
    sleep_for_attr_cache_expiry();
    struct stat after_unlink = require_stat_snapshot(dir);
    check_time_transition("unlink child", before_unlink, after_unlink_immediate,
                          after_unlink,
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
          "[integration][getattr][stat][timestamps]") {
  scenario([&](FdbfsEnv &env) {
    if (is_host_backend()) {
      SKIP("host backend has the passing variant of this test");
    }
    run_timestamp_behavior_matrix(env);
  });
}

TEST_CASE(
    "atime does not go backwards when an older reader closes after a newer "
    "reader",
    "[integration][getattr][stat][timestamps]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("atime-monotonic");

    int seed_fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(seed_fd);
    const std::array<uint8_t, 4> seed = {'a', 'b', 'c', 'd'};
    write_all_fd(seed_fd, seed.data(), seed.size());
    FDBFS_REQUIRE_OK(::close(seed_fd));

    const struct stat initial = require_stat_snapshot(p);

    sleep_for_timestamp_tick();
    int fd1 = ::open(p.c_str(), O_RDONLY);
    FDBFS_REQUIRE_NONNEG(fd1);
    char c1 = '\0';
    FDBFS_REQUIRE_NONNEG(::read(fd1, &c1, 1));

    // Reset the visible atime while fd1 is still open so that a later read on
    // fd2 will advance it again. When fd1 closes afterwards, atime must not
    // regress back to fd1's earlier read time.
    sleep_for_timestamp_tick();
    set_only_atime(p, initial.st_atim);
    const struct stat before_fd2 = require_stat_snapshot(p);
    CHECK(compare_timespec(before_fd2.st_atim, initial.st_atim) == 0);

    sleep_for_timestamp_tick();
    int fd2 = ::open(p.c_str(), O_RDONLY);
    FDBFS_REQUIRE_NONNEG(fd2);
    char c2 = '\0';
    FDBFS_REQUIRE_NONNEG(::read(fd2, &c2, 1));
    FDBFS_REQUIRE_OK(::close(fd2));

    sleep_for_timestamp_tick();

    const struct stat after_fd2 = require_stat_snapshot(p);
    CHECK(compare_timespec(after_fd2.st_atim, before_fd2.st_atim) > 0);

    FDBFS_REQUIRE_OK(::close(fd1));
    const struct stat after_fd1 = require_stat_snapshot(p);
    CHECK(compare_timespec(after_fd1.st_atim, after_fd2.st_atim) >= 0);
  });
}
