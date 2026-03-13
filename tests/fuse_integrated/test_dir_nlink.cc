#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <string>
#include <vector>

#include "test_support.h"

namespace {

nlink_t stat_dir_nlink(const fs::path &p) {
  struct stat st {};
  FDBFS_REQUIRE_OK(::stat(p.c_str(), &st));
  REQUIRE(S_ISDIR(st.st_mode));
  return st.st_nlink;
}

void create_regular_file(const fs::path &p) {
  int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  FDBFS_REQUIRE_NONNEG(fd);
  FDBFS_REQUIRE_OK(::close(fd));
}

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

void check_chain_nlinks(const std::vector<fs::path> &chain) {
  for (size_t i = 0; i < chain.size(); i++) {
    const nlink_t expected = static_cast<nlink_t>(2 + ((i + 1 < chain.size()) ? 1 : 0));
    CHECK(stat_dir_nlink(chain[i]) == expected);
  }
}

} // namespace

TEST_CASE("directory nlink tracks subdirectory create and remove",
          "[integration][nlink][mkdir][rmdir]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path parent = env.p("parent");
    FDBFS_REQUIRE_OK(::mkdir(parent.c_str(), 0755));

    const nlink_t base = stat_dir_nlink(parent);
    REQUIRE(base >= 2);

    std::vector<fs::path> children;
    constexpr int count = 32;
    children.reserve(count);
    for (int i = 0; i < count; i++) {
      const fs::path child = parent / ("d" + std::to_string(i));
      FDBFS_REQUIRE_OK(::mkdir(child.c_str(), 0755));
      children.push_back(child);
      CHECK(stat_dir_nlink(parent) == static_cast<nlink_t>(base + i + 1));
    }

    for (int i = 0; i < count; i++) {
      FDBFS_REQUIRE_OK(::rmdir(children[static_cast<size_t>(i)].c_str()));
      CHECK(stat_dir_nlink(parent) ==
            static_cast<nlink_t>(base + (count - i - 1)));
    }
  });
}

TEST_CASE("directory nlink is unaffected by non-directory entries",
          "[integration][nlink][mkdir][link][symlink]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path parent = env.p("mixed");
    FDBFS_REQUIRE_OK(::mkdir(parent.c_str(), 0755));
    const nlink_t base = stat_dir_nlink(parent);
    REQUIRE(base >= 2);

    std::vector<fs::path> regulars;
    std::vector<fs::path> fifos;
    std::vector<fs::path> symlinks;
    std::vector<fs::path> hardlinks;

    for (int i = 0; i < 8; i++) {
      const fs::path f = parent / ("f_" + std::to_string(i));
      create_regular_file(f);
      regulars.push_back(f);
      CHECK(stat_dir_nlink(parent) == base);
    }

    for (int i = 0; i < 4; i++) {
      const fs::path p = parent / ("fifo_" + std::to_string(i));
      FDBFS_REQUIRE_OK(::mkfifo(p.c_str(), 0644));
      fifos.push_back(p);
      CHECK(stat_dir_nlink(parent) == base);
    }

    for (int i = 0; i < 4; i++) {
      const fs::path p = parent / ("sym_" + std::to_string(i));
      FDBFS_REQUIRE_OK(::symlink("target-does-not-exist", p.c_str()));
      symlinks.push_back(p);
      CHECK(stat_dir_nlink(parent) == base);
    }

    const fs::path src = parent / "hard_src";
    create_regular_file(src);
    CHECK(stat_dir_nlink(parent) == base);
    for (int i = 0; i < 4; i++) {
      const fs::path p = parent / ("hard_" + std::to_string(i));
      FDBFS_REQUIRE_OK(::link(src.c_str(), p.c_str()));
      hardlinks.push_back(p);
      CHECK(stat_dir_nlink(parent) == base);
    }

    for (const auto &p : hardlinks) {
      FDBFS_REQUIRE_OK(::unlink(p.c_str()));
      CHECK(stat_dir_nlink(parent) == base);
    }
    FDBFS_REQUIRE_OK(::unlink(src.c_str()));
    CHECK(stat_dir_nlink(parent) == base);

    for (const auto &p : symlinks) {
      FDBFS_REQUIRE_OK(::unlink(p.c_str()));
      CHECK(stat_dir_nlink(parent) == base);
    }

    for (const auto &p : fifos) {
      FDBFS_REQUIRE_OK(::unlink(p.c_str()));
      CHECK(stat_dir_nlink(parent) == base);
    }

    for (const auto &p : regulars) {
      FDBFS_REQUIRE_OK(::unlink(p.c_str()));
      CHECK(stat_dir_nlink(parent) == base);
    }
  });
}

TEST_CASE("directory nlink is correct across deep chain create/remove",
          "[integration][nlink][mkdir][rmdir]") {
  scenario([&](FdbfsEnv &env) {
    std::vector<fs::path> chain;
    chain.reserve(12);

    fs::path p = env.mnt;
    for (int i = 0; i < 12; i++) {
      p /= ("d" + std::to_string(i));
      FDBFS_REQUIRE_OK(::mkdir(p.c_str(), 0755));
      chain.push_back(p);
      check_chain_nlinks(chain);
    }

    while (!chain.empty()) {
      const fs::path leaf = chain.back();
      FDBFS_REQUIRE_OK(::rmdir(leaf.c_str()));
      chain.pop_back();
      check_chain_nlinks(chain);
    }
  });
}

TEST_CASE("rename directory across parents updates both parent nlinks",
          "[integration][nlink][rename][mkdir]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path a = env.p("A");
    const fs::path b = env.p("B");
    const fs::path ax = a / "x";
    const fs::path bx = b / "x";
    FDBFS_REQUIRE_OK(::mkdir(a.c_str(), 0755));
    FDBFS_REQUIRE_OK(::mkdir(b.c_str(), 0755));
    FDBFS_REQUIRE_OK(::mkdir(ax.c_str(), 0755));

    FDBFS_REQUIRE_OK(::rename(ax.c_str(), bx.c_str()));
    CHECK(!fs::exists(ax));
    CHECK(fs::exists(bx));
    CHECK(fs::is_directory(bx));
    CHECK(stat_dir_nlink(a) == 2);
    CHECK(stat_dir_nlink(b) == 3);
  });
}

TEST_CASE("rename directory over empty destination keeps destination parent nlink",
          "[integration][nlink][rename][mkdir]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path a = env.p("A");
    const fs::path b = env.p("B");
    const fs::path ax = a / "x";
    const fs::path by = b / "y";
    FDBFS_REQUIRE_OK(::mkdir(a.c_str(), 0755));
    FDBFS_REQUIRE_OK(::mkdir(b.c_str(), 0755));
    FDBFS_REQUIRE_OK(::mkdir(ax.c_str(), 0755));
    FDBFS_REQUIRE_OK(::mkdir(by.c_str(), 0755));

    FDBFS_REQUIRE_OK(::rename(ax.c_str(), by.c_str()));
    CHECK(!fs::exists(ax));
    CHECK(fs::exists(by));
    CHECK(fs::is_directory(by));
    CHECK(stat_dir_nlink(a) == 2);
    CHECK(stat_dir_nlink(b) == 3);
  });
}

TEST_CASE("rename directory within same parent keeps parent nlink",
          "[integration][nlink][rename][mkdir]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path parent = env.p("P");
    const fs::path src = parent / "src";
    const fs::path dst = parent / "dst";
    FDBFS_REQUIRE_OK(::mkdir(parent.c_str(), 0755));
    FDBFS_REQUIRE_OK(::mkdir(src.c_str(), 0755));

    const nlink_t before = stat_dir_nlink(parent);
    REQUIRE(before == 3);

    FDBFS_REQUIRE_OK(::rename(src.c_str(), dst.c_str()));
    CHECK(!fs::exists(src));
    CHECK(fs::exists(dst));
    CHECK(fs::is_directory(dst));
    CHECK(stat_dir_nlink(parent) == before);
  });
}

TEST_CASE(
    "rename directory over empty sibling within same parent decreases parent "
    "nlink",
    "[integration][nlink][rename][mkdir]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path parent = env.p("P");
    const fs::path src = parent / "src";
    const fs::path dst = parent / "dst";
    FDBFS_REQUIRE_OK(::mkdir(parent.c_str(), 0755));
    FDBFS_REQUIRE_OK(::mkdir(src.c_str(), 0755));
    FDBFS_REQUIRE_OK(::mkdir(dst.c_str(), 0755));

    const nlink_t before = stat_dir_nlink(parent);
    REQUIRE(before == 4);

    FDBFS_REQUIRE_OK(::rename(src.c_str(), dst.c_str()));
    CHECK(!fs::exists(src));
    CHECK(fs::exists(dst));
    CHECK(fs::is_directory(dst));
    CHECK(stat_dir_nlink(parent) == static_cast<nlink_t>(before - 1));
    CHECK(stat_dir_nlink(dst) == 2);
  });
}

TEST_CASE("rename exchange directory and file across parents updates nlinks",
          "[integration][nlink][rename][mkdir]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path a = env.p("A");
    const fs::path b = env.p("B");
    const fs::path ad = a / "d";
    const fs::path bf = b / "f";
    FDBFS_REQUIRE_OK(::mkdir(a.c_str(), 0755));
    FDBFS_REQUIRE_OK(::mkdir(b.c_str(), 0755));
    FDBFS_REQUIRE_OK(::mkdir(ad.c_str(), 0755));
    create_regular_file(bf);

    errno = 0;
    if (renameat2_wrapper(ad, bf, RENAME_EXCHANGE) == -1) {
      if (errno == ENOSYS) {
        SKIP("renameat2 not available on this platform/kernel");
      }
      FAIL("renameat2(RENAME_EXCHANGE) failed with errno=" << errno);
    }

    CHECK(fs::exists(ad));
    CHECK(fs::exists(bf));
    CHECK(!fs::is_directory(ad));
    CHECK(fs::is_directory(bf));
    CHECK(stat_dir_nlink(a) == 2);
    CHECK(stat_dir_nlink(b) == 3);
  });
}
