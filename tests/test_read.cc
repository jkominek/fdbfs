#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "test_support.h"

TEST_CASE("create write readback matches", "[integration][open][write][read]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("readback.bin");
    const std::string payload = "hello-from-catch2";
    std::vector<char> buf(payload.size(), '\0');

    int fd = ::open(p.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    REQUIRE(fd >= 0);
    REQUIRE(::write(fd, payload.data(), payload.size()) ==
            static_cast<ssize_t>(payload.size()));
    REQUIRE(::close(fd) == 0);

    fd = ::open(p.c_str(), O_RDONLY);
    REQUIRE(fd >= 0);
    REQUIRE(::read(fd, buf.data(), buf.size()) ==
            static_cast<ssize_t>(payload.size()));
    REQUIRE(::close(fd) == 0);

    REQUIRE(std::string(buf.begin(), buf.end()) == payload);
  });
}
