#include <catch2/catch_test_macros.hpp>

#include <sys/stat.h>

#include "test_support.h"

TEST_CASE("mount has root directory", "[integration][mount][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    struct stat st {};
    REQUIRE(::stat(env.mnt.c_str(), &st) == 0);
    REQUIRE(S_ISDIR(st.st_mode));
  });
}
