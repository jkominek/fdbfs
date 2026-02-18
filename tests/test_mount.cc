#include <catch2/catch_test_macros.hpp>

#include <sys/stat.h>
#include <sys/statfs.h>

#include <cstdint>

#include "test_support.h"

namespace {

constexpr long FUSE_SUPER_MAGIC = 0x65735546; // "eUsF"

} // namespace

TEST_CASE("mount root inode baseline", "[integration][mount][stat]") {
  scenario([&](FdbfsEnv &env) {
    struct stat st{};
    FDBFS_REQUIRE_OK(::stat(env.mnt.c_str(), &st));
    CHECK(S_ISDIR(st.st_mode));
    CHECK(st.st_ino == 1);
    CHECK((st.st_mode & 0555) == 0555);
    CHECK(st.st_nlink >= 2);
  });
}

TEST_CASE("mount statfs sanity", "[integration][mount][statfs]") {
  scenario([&](FdbfsEnv &env) {
    struct statfs s{};
    FDBFS_REQUIRE_OK(::statfs(env.mnt.c_str(), &s));
    CHECK(s.f_type == FUSE_SUPER_MAGIC);
    CHECK(s.f_bsize > 1);
    CHECK(s.f_blocks > s.f_bfree);
    CHECK(s.f_blocks > s.f_bavail);
    CHECK(s.f_files > 0);
    CHECK(s.f_ffree > 0);
    CHECK(s.f_namelen >= 255);
  });
}
