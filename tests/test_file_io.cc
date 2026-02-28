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
  const bool matches = (got == expected);
  if (!matches) {
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
  CHECK(matches);
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

TEST_CASE("file IO fsx truncate/read/write/read regression sequence",
          "[integration][read][write][file_io][regression]") {
  scenario([&](FdbfsEnv &env) {
    struct PayloadCase {
      const char *name;
      std::vector<uint8_t> (*make)(size_t);
    };

    const PayloadCase payload_cases[] = {
        {"zeros", [](size_t n) { return std::vector<uint8_t>(n, 0); }},
        {"A-fill", [](size_t n) { return std::vector<uint8_t>(n, 'A'); }},
        {"random", [](size_t n) { return make_pattern(n, 0x0f00ba11ULL); }},
    };

    for (const bool do_preread : {true, false}) {
      for (const auto &payload_case : payload_cases) {
        DYNAMIC_SECTION("preread=" << do_preread
                                   << " payload_case=" << payload_case.name) {
          INFO("preread=" << do_preread
                          << " payload_case=" << payload_case.name);

          const fs::path p = env.p(std::string("fsx-seq-") +
                                   (do_preread ? "preread-" : "nopreread-") +
                                   payload_case.name + ".bin");
          int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
          FDBFS_REQUIRE_NONNEG(fd);

          // 1) TRUNCATE UP from 0x0 to 0x403
          FDBFS_REQUIRE_OK(::ftruncate(fd, 0x403));
          std::vector<uint8_t> expected(0x403, 0);

          // 2) READ 0x8f thru 0x402 (0x374 bytes)
          if (do_preread) {
            const auto preread = pread_exact_fd(fd, 0x374, 0x8f);
            REQUIRE(preread.size() == 0x374);
            CHECK(std::all_of(preread.begin(), preread.end(),
                              [](uint8_t b) { return b == 0; }));
          }

          // 3) WRITE 0x469 thru 0x3fff (0x3b97 bytes)
          const auto payload = payload_case.make(0x3b97);
          pwrite_all_fd(fd, payload.data(), payload.size(), 0x469);
          apply_expected_write(expected, 0x469, payload);

          // 4) READ 0x13d6 thru 0x3fff (0x2c2a bytes)
          const auto got = pread_exact_fd(fd, 0x2c2a, 0x13d6);
          REQUIRE(got.size() == 0x2c2a);
          const std::vector<uint8_t> expected_slice(
              expected.begin() + 0x13d6, expected.begin() + 0x13d6 + 0x2c2a);
          const bool matches = (got == expected_slice);
          if (!matches) {
            size_t mismatch = 0;
            while (mismatch < got.size() &&
                   got[mismatch] == expected_slice[mismatch]) {
              mismatch++;
            }
            INFO("first_mismatch_index=" << mismatch);
            if (mismatch < got.size()) {
              INFO("expected_byte="
                   << static_cast<unsigned>(expected_slice[mismatch])
                   << " got_byte=" << static_cast<unsigned>(got[mismatch]));
            }
          }
          CHECK(matches);

          FDBFS_REQUIRE_OK(::close(fd));
        }
      }
    }
  });
}

TEST_CASE("file IO sparse neighbor block write should not fail with EIO",
          "[integration][read][write][file_io][regression]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("write-neighbor-block-regression.bin");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);

    struct stat st{};
    FDBFS_REQUIRE_OK(::fstat(fd, &st));
    REQUIRE(st.st_blksize > 0);
    const off_t bs = static_cast<off_t>(st.st_blksize);

    // Seed only block 34 so block 33 remains absent/sparse.
    const std::vector<uint8_t> block34_seed = {'A', 'B', 'C', 'D'};
    REQUIRE(::pwrite(fd, block34_seed.data(), block34_seed.size(), 34 * bs) ==
            static_cast<ssize_t>(block34_seed.size()));

    // Partial write into block 33 should succeed and must not depend on block
    // 34's contents.
    const std::vector<uint8_t> block33_patch = {'w', 'x', 'y', 'z'};
    errno = 0;
    const ssize_t wrote =
        ::pwrite(fd, block33_patch.data(), block33_patch.size(), 33 * bs + 123);
    INFO("second pwrite rc=" << wrote << " errno=" << errno_with_message(errno));
    REQUIRE(wrote == static_cast<ssize_t>(block33_patch.size()));

    FDBFS_REQUIRE_OK(::close(fd));
  });
}
