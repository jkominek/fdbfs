#include <catch2/catch_test_macros.hpp>

#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <set>
#include <string>

#include "test_posix_helpers.h"
#include "test_support.h"

TEST_CASE("readdir returns created entries and handles seekdir/telldir",
          "[integration][readdir][mkdir][mknod]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path dir = env.p("scan");
    REQUIRE(::mkdir(dir.c_str(), 0755) == 0);

    const std::string n1 = "alpha";
    const std::string n2(255, 'x');
    const std::string n3 = "nested";
    const std::string n4 = "pipe";

    int fd = ::open((dir / n1).c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);
    REQUIRE(::mkdir((dir / n3).c_str(), 0755) == 0);
    REQUIRE(::mkfifo((dir / n4).c_str(), 0644) == 0);
    fd = ::open((dir / n2).c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

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

    REQUIRE(::closedir(d) == 0);
  });
}

TEST_CASE("readdir on non-directory fails with ENOTDIR",
          "[integration][readdir]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path file = env.p("not_a_dir");
    int fd = ::open(file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::close(fd) == 0);

    errno = 0;
    CHECK(::opendir(file.c_str()) == nullptr);
    CHECK(errno == ENOTDIR);
  });
}

TEST_CASE("readdir handles directories with many entries",
          "[integration][readdir][open]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path dir = env.p("bulk");
    REQUIRE(::mkdir(dir.c_str(), 0755) == 0);

    for (int i = 0; i < 48; i++) {
      const fs::path p = dir / ("entry_" + std::to_string(i));
      int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
      REQUIRE(fd >= 0);
      REQUIRE(::close(fd) == 0);
    }

    const auto names = readdir_names(dir);
    for (int i = 0; i < 48; i++) {
      require_contains(names, "entry_" + std::to_string(i));
    }
  });
}
