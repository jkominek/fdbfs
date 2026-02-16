#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>

#include "test_support.h"

TEST_CASE("create write close stat size", "[integration][open][write][stat]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("hello.txt");
    const std::string payload = "abc123456789";

    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);

    const ssize_t wrote = ::write(fd, payload.data(), payload.size());
    REQUIRE(wrote == static_cast<ssize_t>(payload.size()));
    REQUIRE(::close(fd) == 0);

    struct stat st {};
    REQUIRE(::stat(p.c_str(), &st) == 0);
    REQUIRE(S_ISREG(st.st_mode));
    REQUIRE(static_cast<size_t>(st.st_size) == payload.size());
  });
}
