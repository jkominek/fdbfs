#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "test_support.h"

namespace {

std::vector<std::string> numbered_names(int count) {
  std::vector<std::string> names;
  names.reserve(static_cast<size_t>(count));
  for (int i = 1; i <= count; i++) {
    names.push_back(std::to_string(i));
  }
  return names;
}

} // namespace

TEST_CASE("rmdir removes empty directory and reports expected errors",
          "[integration][rmdir][mkdir][open]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path d = env.p("d");
    FDBFS_REQUIRE_OK(::mkdir(d.c_str(), 0755));
    FDBFS_REQUIRE_OK(::rmdir(d.c_str()));
    CHECK(!fs::exists(d));

    errno = 0;
    CHECK(::rmdir(d.c_str()) == -1);
    FDBFS_CHECK_ERRNO(ENOENT);

    const fs::path nonempty = env.p("nonempty");
    FDBFS_REQUIRE_OK(::mkdir(nonempty.c_str(), 0755));
    const fs::path child = nonempty / "child";
    int fd = ::open(child.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    errno = 0;
    CHECK(::rmdir(nonempty.c_str()) == -1);
    FDBFS_CHECK_ERRNO(ENOTEMPTY);

    FDBFS_REQUIRE_OK(::unlink(child.c_str()));
    FDBFS_REQUIRE_OK(::rmdir(nonempty.c_str()));

    const fs::path file = env.p("file");
    fd = ::open(file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    errno = 0;
    CHECK(::rmdir(file.c_str()) == -1);
    FDBFS_CHECK_ERRNO(ENOTDIR);
  });
}

TEST_CASE("rmdir mkdir-rmdir immediate loop x32",
          "[integration][rmdir][mkdir][stress]") {
  scenario([&](FdbfsEnv &env) {
    const auto names = numbered_names(32);
    for (const auto &name : names) {
      const fs::path d = env.p(name);
      FDBFS_REQUIRE_OK(::mkdir(d.c_str(), 0755));
      FDBFS_REQUIRE_OK(::rmdir(d.c_str()));
    }
  });
}

TEST_CASE("rmdir mkdir all then rmdir all x32",
          "[integration][rmdir][mkdir][stress]") {
  scenario([&](FdbfsEnv &env) {
    const auto names = numbered_names(32);
    for (const auto &name : names) {
      const fs::path d = env.p(name);
      FDBFS_REQUIRE_OK(::mkdir(d.c_str(), 0755));
    }
    for (const auto &name : names) {
      const fs::path d = env.p(name);
      FDBFS_REQUIRE_OK(::rmdir(d.c_str()));
    }
  });
}

TEST_CASE("rmdir mkdir and rmdir in shuffled order x32",
          "[integration][rmdir][mkdir][stress]") {
  scenario([&](FdbfsEnv &env) {
    auto create_names = numbered_names(32);
    auto remove_names = create_names;

    std::mt19937_64 create_rng(0x4d495244554c4c31ULL);
    std::mt19937_64 remove_rng(0x4d495244554c4c32ULL);
    std::shuffle(create_names.begin(), create_names.end(), create_rng);
    std::shuffle(remove_names.begin(), remove_names.end(), remove_rng);

    for (const auto &name : create_names) {
      const fs::path d = env.p(name);
      FDBFS_REQUIRE_OK(::mkdir(d.c_str(), 0755));
    }
    for (const auto &name : remove_names) {
      const fs::path d = env.p(name);
      FDBFS_REQUIRE_OK(::rmdir(d.c_str()));
    }
  });
}

TEST_CASE("rmdir preserves usability for open/cwd holders while blocking creates",
          "[integration][rmdir][mkdir][open]") {
  scenario([&](FdbfsEnv &env) {
    SECTION("directory held by open fd") {
      const fs::path d = env.p("held-open");
      FDBFS_REQUIRE_OK(::mkdir(d.c_str(), 0755));

      const int dfd = ::open(d.c_str(), O_RDONLY | O_DIRECTORY);
      FDBFS_REQUIRE_NONNEG(dfd);

      FDBFS_REQUIRE_OK(::rmdir(d.c_str()));
      ::usleep(500 * 1000);

      struct stat st {};
      FDBFS_REQUIRE_OK(::fstat(dfd, &st));
      CHECK(S_ISDIR(st.st_mode));
      CHECK(st.st_nlink <= 1);

      errno = 0;
      const int created = ::openat(dfd, "new-child", O_CREAT | O_WRONLY, 0644);
      CHECK(created == -1);
      FDBFS_CHECK_ERRNO(ENOENT);
      if (created >= 0) {
        FDBFS_REQUIRE_OK(::close(created));
      }

      FDBFS_REQUIRE_OK(::close(dfd));
      CHECK(!fs::exists(d));
    }

    SECTION("directory held as another process cwd") {
      const fs::path d = env.p("held-cwd");
      FDBFS_REQUIRE_OK(::mkdir(d.c_str(), 0755));

      int ready_pipe[2] = {-1, -1};
      int go_pipe[2] = {-1, -1};
      int result_pipe[2] = {-1, -1};
      FDBFS_REQUIRE_OK(::pipe(ready_pipe));
      FDBFS_REQUIRE_OK(::pipe(go_pipe));
      FDBFS_REQUIRE_OK(::pipe(result_pipe));

      struct ChildResult {
        int stat_rc;
        int stat_errno;
        int create_rc;
        int create_errno;
      };

      const pid_t child = ::fork();
      FDBFS_REQUIRE_NONNEG(child);
      if (child == 0) {
        (void)::close(ready_pipe[0]);
        (void)::close(go_pipe[1]);
        (void)::close(result_pipe[0]);

        ChildResult result{};

        if (::chdir(d.c_str()) != 0) {
          result.stat_rc = -1;
          result.stat_errno = errno;
          result.create_rc = -1;
          result.create_errno = errno;
          (void)::write(result_pipe[1], &result, sizeof(result));
          _exit(2);
        }

        uint8_t ready = 1;
        (void)::write(ready_pipe[1], &ready, sizeof(ready));

        uint8_t go = 0;
        const ssize_t got = ::read(go_pipe[0], &go, sizeof(go));
        if (got != static_cast<ssize_t>(sizeof(go))) {
          _exit(3);
        }

        struct stat st {};
        errno = 0;
        result.stat_rc = ::stat(".", &st);
        result.stat_errno = errno;

        errno = 0;
        result.create_rc = ::open("new-child", O_CREAT | O_WRONLY, 0644);
        result.create_errno = errno;
        if (result.create_rc >= 0) {
          (void)::close(result.create_rc);
        }

        (void)::write(result_pipe[1], &result, sizeof(result));
        _exit(0);
      }

      (void)::close(ready_pipe[1]);
      (void)::close(go_pipe[0]);
      (void)::close(result_pipe[1]);

      uint8_t ready = 0;
      const ssize_t ready_n = ::read(ready_pipe[0], &ready, sizeof(ready));
      REQUIRE(ready_n == static_cast<ssize_t>(sizeof(ready)));

      FDBFS_REQUIRE_OK(::rmdir(d.c_str()));
      ::usleep(500 * 1000);

      uint8_t go = 1;
      REQUIRE(::write(go_pipe[1], &go, sizeof(go)) ==
              static_cast<ssize_t>(sizeof(go)));

      ChildResult result{};
      const ssize_t result_n = ::read(result_pipe[0], &result, sizeof(result));
      REQUIRE(result_n == static_cast<ssize_t>(sizeof(result)));

      CHECK(result.stat_rc == 0);
      CHECK(result.create_rc == -1);
      CHECK(result.create_errno == ENOENT);

      int status = 0;
      FDBFS_REQUIRE_NONNEG(::waitpid(child, &status, 0));
      CHECK(WIFEXITED(status));
      CHECK(WEXITSTATUS(status) == 0);

      FDBFS_REQUIRE_OK(::close(ready_pipe[0]));
      FDBFS_REQUIRE_OK(::close(go_pipe[1]));
      FDBFS_REQUIRE_OK(::close(result_pipe[0]));

      CHECK(!fs::exists(d));
    }
  });
}
