#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <ctime>
#include <source_location>
#include <string>
#include <vector>

#include "test_posix_helpers.h"
#include "test_support.h"

TEST_CASE("open existing files read-only succeeds across many files",
          "[integration][open][stat]") {
  scenario([&](FdbfsEnv &env) {
    for (int i = 0; i < 256; i++) {
      const fs::path p = env.p("existing_" + std::to_string(i));

      int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
      FDBFS_REQUIRE_NONNEG(fd);
      FDBFS_REQUIRE_OK(::close(fd));

      fd = ::open(p.c_str(), O_RDONLY);
      FDBFS_REQUIRE_NONNEG(fd);
      FDBFS_REQUIRE_OK(::close(fd));

      struct stat st{};
      require_stat_mode(p, st, S_IFREG);
    }
  });
}

TEST_CASE("open missing file without O_CREAT returns ENOENT",
          "[integration][open]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("missing");
    errno = 0;
    CHECK(::open(p.c_str(), O_RDONLY) == -1);
    FDBFS_CHECK_ERRNO(ENOENT);
  });
}

TEST_CASE("open O_CREAT respects requested modes",
          "[integration][open][stat]") {
  scenario([&](FdbfsEnv &env) {
    const mode_t um = current_umask();
    const std::vector<mode_t> requested_modes = {0000, 0001, 0010, 0100,
                                                 0555, 0700, 0751, 0777};
    for (size_t i = 0; i < requested_modes.size(); i++) {
      const fs::path p = env.p("mode_" + std::to_string(i));
      const mode_t expected_mode = (requested_modes[i] & ~um) & 0777;
      int fd =
          ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, requested_modes[i]);
      FDBFS_REQUIRE_NONNEG(fd);
      FDBFS_REQUIRE_OK(::close(fd));

      struct stat st{};
      require_stat_mode(p, st, S_IFREG);
      CHECK((st.st_mode & 0777) == expected_mode);
    }
  });
}

TEST_CASE("open O_CREAT O_EXCL on existing file returns EEXIST",
          "[integration][open]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("dupe");
    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    errno = 0;
    CHECK(::open(p.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644) == -1);
    FDBFS_CHECK_ERRNO(EEXIST);
  });
}

TEST_CASE("open O_CREAT updates parent ctime/mtime",
          "[integration][open][stat]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path parent = env.p("parent_create");
    FDBFS_REQUIRE_OK(::mkdir(parent.c_str(), 0755));

    struct stat pst_before{};
    FDBFS_REQUIRE_OK(::stat(parent.c_str(), &pst_before));
    ::sleep(1);

    const fs::path child = parent / "newfile";
    int fd = ::open(child.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    struct stat pst_after{};
    FDBFS_REQUIRE_OK(::stat(parent.c_str(), &pst_after));
    CHECK(compare_timespec(pst_after.st_mtim, pst_before.st_mtim) > 0);
    CHECK(compare_timespec(pst_after.st_ctim, pst_before.st_ctim) > 0);
  });
}

TEST_CASE("open O_NOATIME prevents file atime updates",
          "[integration][open][stat]") {
  scenario([&](FdbfsEnv &env) {
#ifndef O_NOATIME
    SKIP("O_NOATIME is not available on this platform");
    return;
#endif
    if (is_host_backend()) {
      SKIP("host backend does not exercise fdbfs O_NOATIME handling");
    }

    const fs::path p = env.p("noatime");
    const std::string payload = "x";

    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    REQUIRE(::write(fd, payload.data(), payload.size()) ==
            static_cast<ssize_t>(payload.size()));
    FDBFS_REQUIRE_OK(::close(fd));

    struct stat before{};
    require_stat_mode(p, before, S_IFREG);

    ::sleep(1);

    fd = ::open(p.c_str(), O_RDONLY | O_NOATIME);
    FDBFS_REQUIRE_NONNEG(fd);
    char c = '\0';
    REQUIRE(::read(fd, &c, 1) == 1);
    FDBFS_REQUIRE_OK(::close(fd));

    struct timespec ts{
        .tv_sec = 0,
        .tv_nsec = 50 * 1000 * 1000,
    };
    FDBFS_REQUIRE_OK(::nanosleep(&ts, nullptr));

    struct stat after{};
    require_stat_mode(p, after, S_IFREG);
    CHECK(compare_timespec(after.st_atim, before.st_atim) == 0);
    CHECK(compare_timespec(after.st_mtim, before.st_mtim) == 0);
    CHECK(compare_timespec(after.st_ctim, before.st_ctim) == 0);
  });
}

TEST_CASE("open existing file does not update parent ctime/mtime",
          "[integration][open][stat]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path parent = env.p("parent_existing");
    FDBFS_REQUIRE_OK(::mkdir(parent.c_str(), 0755));

    const fs::path child = parent / "existing";
    int fd = ::open(child.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    struct stat pst_before{};
    FDBFS_REQUIRE_OK(::stat(parent.c_str(), &pst_before));
    ::sleep(1);

    fd = ::open(child.c_str(), O_RDONLY);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    struct stat pst_after{};
    FDBFS_REQUIRE_OK(::stat(parent.c_str(), &pst_after));
    CHECK(compare_timespec(pst_after.st_mtim, pst_before.st_mtim) == 0);
    CHECK(compare_timespec(pst_after.st_ctim, pst_before.st_ctim) == 0);
  });
}

TEST_CASE("open O_TRUNC truncates non-empty file and updates file times",
          "[integration][open][write][stat]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("truncate_me");
    const std::string payload = "this will be truncated";

    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    REQUIRE(::write(fd, payload.data(), payload.size()) ==
            static_cast<ssize_t>(payload.size()));
    FDBFS_REQUIRE_OK(::close(fd));

    struct stat st{};
    require_stat_mode(p, st, S_IFREG);
    CHECK(st.st_size == static_cast<off_t>(payload.size()));
    const struct timespec old_mtim = st.st_mtim;
    const struct timespec old_ctim = st.st_ctim;

    ::sleep(1);
    fd = ::open(p.c_str(), O_WRONLY | O_TRUNC);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    FDBFS_REQUIRE_OK(::stat(p.c_str(), &st));
    CHECK(st.st_size == 0);
    CHECK(compare_timespec(st.st_mtim, old_mtim) > 0);
    CHECK(compare_timespec(st.st_ctim, old_ctim) > 0);
  });
}

TEST_CASE("open O_TRUNC on already-empty file keeps size and file times",
          "[integration][open][write][stat]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("already_empty");

    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    struct stat st_before{};
    require_stat_mode(p, st_before, S_IFREG);
    CHECK(st_before.st_size == 0);

    ::sleep(1);
    fd = ::open(p.c_str(), O_WRONLY | O_TRUNC);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    struct stat st_after{};
    require_stat_mode(p, st_after, S_IFREG);
    CHECK(st_after.st_size == 0);
    if (is_host_backend()) {
      // other filesystems may increase the timestamps if they truncate
      // an already empty file.
      CHECK(compare_timespec(st_after.st_mtim, st_before.st_mtim) >= 0);
      CHECK(compare_timespec(st_after.st_ctim, st_before.st_ctim) >= 0);
    } else {
      // fdbfs doesn't modify times unless something else changes
      CHECK(compare_timespec(st_after.st_mtim, st_before.st_mtim) == 0);
      CHECK(compare_timespec(st_after.st_ctim, st_before.st_ctim) == 0);
    }
  });
}

TEST_CASE("open through existing file path component returns ENOTDIR",
          "[integration][open]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path file = env.p("foo");
    int fd = ::open(file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    const fs::path invalid = env.p("foo/bar");
    errno = 0;
    CHECK(::open(invalid.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644) == -1);
    FDBFS_CHECK_ERRNO(ENOTDIR);
  });
}

TEST_CASE("open through missing parent returns ENOENT", "[integration][open]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path missing_parent = env.p("doesnotexist/name");
    errno = 0;
    CHECK(::open(missing_parent.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644) ==
          -1);
    FDBFS_CHECK_ERRNO(ENOENT);
  });
}

TEST_CASE("open too-long filename returns ENAMETOOLONG",
          "[integration][open]") {
  scenario([&](FdbfsEnv &env) {
    const std::string max_name(255, 'b');
    const std::string too_long_name(256, 'b');

    int fd =
        ::open(env.p(max_name).c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    errno = 0;
    CHECK(::open(env.p(too_long_name).c_str(), O_CREAT | O_WRONLY | O_TRUNC,
                 0644) == -1);
    FDBFS_CHECK_ERRNO(ENAMETOOLONG);
  });
}

TEST_CASE("open directory for writing fails", "[integration][open][mkdir]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path dir = env.p("dir_write_fail");
    FDBFS_REQUIRE_OK(::mkdir(dir.c_str(), 0755));

    errno = 0;
    CHECK(::open(dir.c_str(), O_WRONLY) == -1);
    FDBFS_CHECK_ERRNO(EISDIR);
  });
}
