#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <source_location>
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

void verify_whole_file(
    const fs::path &p, const std::vector<uint8_t> &expected,
    std::source_location loc = std::source_location::current()) {
  INFO("verify_whole_file caller=" << loc.file_name() << ":" << loc.line());
  INFO("path=" << p);
  const auto got = read_file_all(p);
  INFO("expected_size=" << expected.size() << " got_size=" << got.size());
  REQUIRE(got.size() == expected.size());
  if (got != expected) {
    size_t mismatch = 0;
    while (mismatch < got.size() && got[mismatch] == expected[mismatch]) {
      mismatch++;
    }
    INFO("first_mismatch_index=" << mismatch);
    if (mismatch < got.size()) {
      INFO("expected_byte=" << static_cast<unsigned>(expected[mismatch])
                            << " got_byte="
                            << static_cast<unsigned>(got[mismatch]));
    }
  }
  CHECK(got == expected);
}

} // namespace

TEST_CASE("file IO mixed aligned/unaligned writes and reads",
          "[integration][read][write][open][setattr]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("io.bin");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);

    struct stat st {};
    FDBFS_REQUIRE_OK(::fstat(fd, &st));
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
    FDBFS_REQUIRE_OK(::ftruncate(fd, trunc_to));
    expected.resize(static_cast<size_t>(trunc_to));
    verify_whole_file(p, expected);

    FDBFS_REQUIRE_OK(::close(fd));

    fd = ::open(p.c_str(), O_RDWR | O_TRUNC);
    FDBFS_REQUIRE_NONNEG(fd);
    expected.clear();
    verify_whole_file(p, expected);

    const auto final_payload = make_pattern(bs + 13, 0x9999ull);
    write_all_fd(fd, final_payload.data(), final_payload.size());
    apply_expected_write(expected, 0, final_payload);
    FDBFS_REQUIRE_OK(::close(fd));
    verify_whole_file(p, expected);
  });
}

TEST_CASE("file remains accessible through open fds after unlink",
          "[integration][read][write][open][unlink]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("unlink-open.bin");

    int fd1 = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd1);

    const std::vector<uint8_t> first = {'a', 'l', 'p', 'h', 'a', '\0', '1'};
    pwrite_all_fd(fd1, first.data(), first.size(), 0);
    FDBFS_REQUIRE_OK(::fsync(fd1));
    ::usleep(500 * 1000);

    int fd2 = ::open(p.c_str(), O_RDONLY);
    FDBFS_REQUIRE_NONNEG(fd2);

    auto got_first = pread_exact_fd(fd2, first.size(), 0);
    REQUIRE(got_first.size() == first.size());
    CHECK(got_first == first);

    FDBFS_REQUIRE_OK(::unlink(p.c_str()));
    ::usleep(500 * 1000);

    errno = 0;
    const int fd3 = ::open(p.c_str(), O_RDONLY);
    CHECK(fd3 < 0);
    FDBFS_CHECK_ERRNO(ENOENT);
    if (fd3 >= 0) {
      FDBFS_REQUIRE_OK(::close(fd3));
    }

    errno = 0;
    CHECK(::access(p.c_str(), F_OK) != 0);
    FDBFS_CHECK_ERRNO(ENOENT);

    const std::vector<uint8_t> second = {'b', 'e', 't', 'a', '\0', '2'};
    pwrite_all_fd(fd1, second.data(), second.size(),
                  static_cast<off_t>(first.size()));
    FDBFS_REQUIRE_OK(::fsync(fd1));
    ::usleep(500 * 1000);

    std::vector<uint8_t> expected = first;
    expected.insert(expected.end(), second.begin(), second.end());
    auto got_after_unlink = pread_exact_fd(fd2, expected.size(), 0);
    REQUIRE(got_after_unlink.size() == expected.size());
    CHECK(got_after_unlink == expected);

    struct stat st {};
    FDBFS_REQUIRE_OK(::fstat(fd1, &st));
    CHECK(st.st_nlink == 0);

    FDBFS_REQUIRE_OK(::close(fd2));
    FDBFS_REQUIRE_OK(::close(fd1));

    errno = 0;
    CHECK(::access(p.c_str(), F_OK) != 0);
    FDBFS_CHECK_ERRNO(ENOENT);
  });
}
