#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
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
