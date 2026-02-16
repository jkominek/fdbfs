#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "test_posix_helpers.h"
#include "test_support.h"

namespace {

void ensure_size(std::vector<uint8_t> &buf, size_t size) {
  if (buf.size() < size) {
    buf.resize(size, 0);
  }
}

void apply_expected_write(std::vector<uint8_t> &expected, off_t off,
                          const std::vector<uint8_t> &payload) {
  const size_t start = static_cast<size_t>(off);
  ensure_size(expected, start + payload.size());
  std::copy(payload.begin(), payload.end(), expected.begin() + start);
}

void verify_whole_file(const fs::path &p, const std::vector<uint8_t> &expected) {
  const auto got = read_file_all(p);
  REQUIRE(got.size() == expected.size());
  CHECK(got == expected);
}

} // namespace

TEST_CASE("file IO mixed aligned/unaligned writes and reads",
          "[integration][read][write][open][setattr]") {
  const fs::path fs_exe = required_env_path("FDBFS_FS_EXE");
  const fs::path source_dir = required_env_path("FDBFS_SOURCE_DIR");

  scenario(fs_exe, source_dir, [&](FdbfsEnv &env) {
    const fs::path p = env.p("io.bin");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    REQUIRE(fd >= 0);

    struct stat st {};
    REQUIRE(::fstat(fd, &st) == 0);
    REQUIRE(st.st_blksize > 0);
    const size_t bs = static_cast<size_t>(st.st_blksize);

    std::vector<uint8_t> expected;

    const std::vector<uint8_t> simple = {'h', 'e', 'l', 'l', 'o', '\0', 'x'};
    pwrite_all_fd(fd, simple.data(), simple.size(), 0);
    apply_expected_write(expected, 0, simple);
    verify_whole_file(p, expected);

    const auto incompressible1 = make_pattern(bs + 73, 0x1111ull);
    pwrite_all_fd(fd, incompressible1.data(), incompressible1.size(),
                  static_cast<off_t>(bs / 2));
    apply_expected_write(expected, static_cast<off_t>(bs / 2), incompressible1);
    verify_whole_file(p, expected);

    const std::vector<uint8_t> zeros(bs * 2 + 11, 0);
    pwrite_all_fd(fd, zeros.data(), zeros.size(), static_cast<off_t>(bs * 3));
    apply_expected_write(expected, static_cast<off_t>(bs * 3), zeros);
    verify_whole_file(p, expected);

    const auto incompressible2 = make_pattern(bs * 5 + 19, 0x5555ull);
    pwrite_all_fd(fd, incompressible2.data(), incompressible2.size(),
                  static_cast<off_t>(bs * 7 + 3));
    apply_expected_write(expected, static_cast<off_t>(bs * 7 + 3),
                         incompressible2);
    verify_whole_file(p, expected);

    const std::vector<uint8_t> mid = {'A', 'B', 'C', 'D', 'E'};
    pwrite_all_fd(fd, mid.data(), mid.size(), static_cast<off_t>(bs + 1));
    apply_expected_write(expected, static_cast<off_t>(bs + 1), mid);
    verify_whole_file(p, expected);

    for (size_t i = 0; i < expected.size(); i += std::max<size_t>(1, bs / 3)) {
      const size_t chunk = std::min<size_t>(127, expected.size() - i);
      const auto got = pread_exact_fd(fd, chunk, static_cast<off_t>(i));
      REQUIRE(got.size() == chunk);
      CHECK(std::memcmp(got.data(), expected.data() + i, chunk) == 0);
    }

    const auto eof_read = pread_exact_fd(fd, 64, static_cast<off_t>(expected.size()));
    CHECK(eof_read.empty());

    const off_t trunc_to = static_cast<off_t>(bs * 2 + 5);
    REQUIRE(::ftruncate(fd, trunc_to) == 0);
    expected.resize(static_cast<size_t>(trunc_to));
    verify_whole_file(p, expected);

    REQUIRE(::close(fd) == 0);

    fd = ::open(p.c_str(), O_RDWR | O_TRUNC);
    REQUIRE(fd >= 0);
    expected.clear();
    verify_whole_file(p, expected);

    const auto final_payload = make_pattern(bs + 13, 0x9999ull);
    write_all_fd(fd, final_payload.data(), final_payload.size());
    apply_expected_write(expected, 0, final_payload);
    REQUIRE(::close(fd) == 0);
    verify_whole_file(p, expected);
  });
}
