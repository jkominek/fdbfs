#include <catch2/catch_test_macros.hpp>

#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "test_posix_helpers.h"
#include "test_support.h"

namespace {

std::optional<std::string> readdir_next_name(DIR *d) {
  errno = 0;
  struct dirent *ent = ::readdir(d);
  if (ent == nullptr) {
    INFO("readdir errno=" << errno_with_message(errno));
    REQUIRE(errno == 0);
    return std::nullopt;
  }
  return std::string(ent->d_name);
}

std::optional<std::string> readdir_next_non_dot(DIR *d) {
  while (true) {
    errno = 0;
    struct dirent *ent = ::readdir(d);
    if (ent == nullptr) {
      INFO("readdir errno=" << errno_with_message(errno));
      REQUIRE(errno == 0);
      return std::nullopt;
    }
    if ((std::string_view(ent->d_name) == ".") ||
        (std::string_view(ent->d_name) == "..")) {
      continue;
    }
    return std::string(ent->d_name);
  }
}

} // namespace

TEST_CASE("readdir returns created entries and handles seekdir/telldir",
          "[integration][readdir][mkdir][mknod]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path dir = env.p("scan");
    FDBFS_REQUIRE_OK(::mkdir(dir.c_str(), 0755));

    const std::string n1 = "alpha";
    const std::string n2(255, 'x');
    const std::string n3 = "nested";
    const std::string n4 = "pipe";

    int fd = ::open((dir / n1).c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));
    FDBFS_REQUIRE_OK(::mkdir((dir / n3).c_str(), 0755));
    FDBFS_REQUIRE_OK(::mkfifo((dir / n4).c_str(), 0644));
    fd = ::open((dir / n2).c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    DIR *d = ::opendir(dir.c_str());
    REQUIRE(d != nullptr);

    std::set<std::string> names;
    long checkpoint = -1;

    while (true) {
      errno = 0;
      struct dirent *ent = ::readdir(d);
      if (!ent) {
        REQUIRE(errno == 0);
        break;
      }
      names.emplace(ent->d_name);
      if (checkpoint == -1 && std::string(ent->d_name) != "." &&
          std::string(ent->d_name) != "..") {
        checkpoint = ::telldir(d);
      }
    }

    require_contains(names, n1);
    require_contains(names, n2);
    require_contains(names, n3);
    require_contains(names, n4);

    if (checkpoint >= 0) {
      ::seekdir(d, checkpoint);
      errno = 0;
      struct dirent *ent = ::readdir(d);
      REQUIRE(ent != nullptr);
      REQUIRE(errno == 0);
    }

    FDBFS_REQUIRE_OK(::closedir(d));
  });
}

TEST_CASE("readdir on non-directory fails with ENOTDIR",
          "[integration][readdir]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path file = env.p("not_a_dir");
    int fd = ::open(file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);
    FDBFS_REQUIRE_OK(::close(fd));

    errno = 0;
    CHECK(::opendir(file.c_str()) == nullptr);
    FDBFS_CHECK_ERRNO(ENOTDIR);
  });
}

TEST_CASE("readdir handles directories with many entries",
          "[integration][readdir][open]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path dir = env.p("bulk");
    FDBFS_REQUIRE_OK(::mkdir(dir.c_str(), 0755));

    for (int i = 0; i < 48; i++) {
      const fs::path p = dir / ("entry_" + std::to_string(i));
      int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
      FDBFS_REQUIRE_NONNEG(fd);
      FDBFS_REQUIRE_OK(::close(fd));
    }

    const auto names = readdir_names(dir);
    for (int i = 0; i < 48; i++) {
      require_contains(names, "entry_" + std::to_string(i));
    }
  });
}

TEST_CASE("readdir restart positions survive deleting earlier entries",
          "[integration][readdir][seekdir][telldir][unlink]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path dir = env.p("resume_after_delete");
    FDBFS_REQUIRE_OK(::mkdir(dir.c_str(), 0755));

    for (std::string_view name : {"a", "b", "c", "d", "e", "f"}) {
      int fd = ::open((dir / name).c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
      FDBFS_REQUIRE_NONNEG(fd);
      FDBFS_REQUIRE_OK(::close(fd));
    }

    DIR *d = ::opendir(dir.c_str());
    REQUIRE(d != nullptr);

    std::vector<std::string> observed;
    std::vector<long> checkpoints;
    while (auto name = readdir_next_non_dot(d)) {
      observed.push_back(*name);
      checkpoints.push_back(::telldir(d));
    }

    REQUIRE(observed.size() == 6);
    REQUIRE(checkpoints.size() == observed.size());

    auto check_resume_after_deletes = [&](size_t expected_index) {
      INFO("expected_index=" << expected_index);
      INFO("expected_name=" << observed[expected_index]);

      for (size_t i = 0; i < expected_index; i++) {
        INFO("deleting_behind=" << observed[i]);
        FDBFS_REQUIRE_OK(::unlink((dir / observed[i]).c_str()));
      }

      ::seekdir(d, checkpoints[expected_index - 1]);

      std::vector<std::string> resumed;
      while (auto name = readdir_next_non_dot(d)) {
        resumed.push_back(*name);
      }

      std::vector<std::string> expected_suffix(observed.begin() + expected_index,
                                               observed.end());
      INFO("resumed_size=" << resumed.size());
      REQUIRE(resumed == expected_suffix);
    };

    SECTION("resume near beginning") { check_resume_after_deletes(1); }
    SECTION("resume from middle") { check_resume_after_deletes(3); }
    SECTION("resume at final entry") { check_resume_after_deletes(5); }

    FDBFS_REQUIRE_OK(::closedir(d));
  });
}

TEST_CASE("readdir dot entries stay at the front and seekdir can re-enter them",
          "[integration][readdir][seekdir][telldir][dot]") {
  scenario([&](FdbfsEnv &env) {
    if (is_host_backend()) {
      SKIP("host backend does not guarantee our synthetic dot-entry ordering");
    }

    const fs::path dir = env.p("dotdot_front");
    FDBFS_REQUIRE_OK(::mkdir(dir.c_str(), 0755));

    for (std::string_view name :
         {" leading-space", "-dash", ",comma", ".-between", "alpha"}) {
      int fd = ::open((dir / name).c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
      FDBFS_REQUIRE_NONNEG(fd);
      FDBFS_REQUIRE_OK(::close(fd));
    }

    DIR *d = ::opendir(dir.c_str());
    REQUIRE(d != nullptr);

    auto first = readdir_next_name(d);
    REQUIRE(first.has_value());
    CHECK(*first == ".");
    const long after_dot = ::telldir(d);

    auto second = readdir_next_name(d);
    REQUIRE(second.has_value());
    CHECK(*second == "..");
    const long after_dotdot = ::telldir(d);

    auto third = readdir_next_name(d);
    REQUIRE(third.has_value());
    CHECK(*third != ".");
    CHECK(*third != "..");

    ::seekdir(d, after_dot);
    auto resumed_after_dot = readdir_next_name(d);
    REQUIRE(resumed_after_dot.has_value());
    CHECK(*resumed_after_dot == "..");

    ::seekdir(d, after_dotdot);
    auto resumed_after_dotdot = readdir_next_name(d);
    REQUIRE(resumed_after_dotdot.has_value());
    CHECK(*resumed_after_dotdot != ".");
    CHECK(*resumed_after_dotdot != "..");

    const auto names = readdir_names(dir);
    CHECK(names.count(".") == 1);
    CHECK(names.count("..") == 1);
    require_contains(names, " leading-space");
    require_contains(names, "-dash");
    require_contains(names, ",comma");
    require_contains(names, ".-between");
    require_contains(names, "alpha");

    FDBFS_REQUIRE_OK(::closedir(d));
  });
}
