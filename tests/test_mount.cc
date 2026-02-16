#include <catch2/catch_test_macros.hpp>

#include <sys/stat.h>
#include <sys/statfs.h>

#include <cstdint>

#include "test_support.h"

namespace {

constexpr long FUSE_SUPER_MAGIC = 0x65735546; // "eUsF"

} // namespace

TEST_CASE("mount root inode baseline", "[integration][mount][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    struct stat st{};
    REQUIRE(::stat(env.mnt.c_str(), &st) == 0);
    CHECK(S_ISDIR(st.st_mode));
    CHECK(st.st_ino == 1);
    CHECK((st.st_mode & 0555) == 0555);
    CHECK(st.st_nlink >= 2);
  });
}

TEST_CASE("mount statfs sanity", "[integration][mount][statfs]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    struct statfs s{};
    REQUIRE(::statfs(env.mnt.c_str(), &s) == 0);
    CHECK(s.f_type == FUSE_SUPER_MAGIC);
    CHECK(s.f_bsize > 1);
    CHECK(s.f_blocks > s.f_bfree);
    CHECK(s.f_blocks > s.f_bavail);
    CHECK(s.f_files > 0);
    CHECK(s.f_ffree > 0);
    CHECK(s.f_namelen >= 255);
  });
}
