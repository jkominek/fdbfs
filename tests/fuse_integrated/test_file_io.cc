#include <catch2/catch_test_macros.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <optional>
#include <source_location>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
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

struct AppendRecordHeader {
  uint64_t magic;
  uint64_t thread_id;
  uint64_t sequence;
  uint32_t payload_size;
  uint32_t reserved;
  uint64_t payload_hash;
  uint64_t header_hash;
};

static_assert(std::is_trivially_copyable_v<AppendRecordHeader>);
static_assert(sizeof(AppendRecordHeader) == 48);

constexpr uint64_t kAppendRecordMagic = 0x7f4cfb1120a95d63ULL;

uint64_t fnv1a_hash(std::span<const uint8_t> bytes) {
  uint64_t h = 14695981039346656037ULL;
  for (uint8_t b : bytes) {
    h ^= static_cast<uint64_t>(b);
    h *= 1099511628211ULL;
  }
  return h;
}

template <typename T> uint64_t fnv1a_append(uint64_t h, const T &value) {
  static_assert(std::is_trivially_copyable_v<T>);
  const auto *ptr = reinterpret_cast<const uint8_t *>(&value);
  for (size_t i = 0; i < sizeof(T); i++) {
    h ^= static_cast<uint64_t>(ptr[i]);
    h *= 1099511628211ULL;
  }
  return h;
}

uint64_t hash_append_header(const AppendRecordHeader &hdr) {
  uint64_t h = 14695981039346656037ULL;
  h = fnv1a_append(h, hdr.magic);
  h = fnv1a_append(h, hdr.thread_id);
  h = fnv1a_append(h, hdr.sequence);
  h = fnv1a_append(h, hdr.payload_size);
  h = fnv1a_append(h, hdr.reserved);
  h = fnv1a_append(h, hdr.payload_hash);
  return h;
}

uint64_t mix64(uint64_t x) {
  x ^= x >> 30;
  x *= 0xbf58476d1ce4e5b9ULL;
  x ^= x >> 27;
  x *= 0x94d049bb133111ebULL;
  x ^= x >> 31;
  return x;
}

std::vector<uint8_t> append_payload(uint64_t thread_id, uint64_t sequence,
                                    size_t size) {
  std::vector<uint8_t> payload(size);
  uint64_t state = mix64(thread_id ^ (sequence * 0x9e3779b97f4a7c15ULL));
  for (size_t i = 0; i < size; i++) {
    state = state * 6364136223846793005ULL + 1;
    payload[i] = static_cast<uint8_t>(state >> 56);
  }
  return payload;
}

size_t append_payload_size(uint64_t thread_id, uint64_t sequence,
                           size_t max_payload_size) {
  return 1 +
         static_cast<size_t>(mix64(thread_id + sequence) % max_payload_size);
}

std::vector<uint8_t> make_append_record(uint64_t thread_id, uint64_t sequence,
                                        size_t payload_size) {
  std::vector<uint8_t> payload =
      append_payload(thread_id, sequence, payload_size);

  AppendRecordHeader hdr{};
  hdr.magic = kAppendRecordMagic;
  hdr.thread_id = thread_id;
  hdr.sequence = sequence;
  hdr.payload_size = static_cast<uint32_t>(payload.size());
  hdr.reserved = 0;
  hdr.payload_hash = fnv1a_hash(payload);
  hdr.header_hash = hash_append_header(hdr);

  std::vector<uint8_t> record(sizeof(hdr) + payload.size());
  std::memcpy(record.data(), &hdr, sizeof(hdr));
  std::memcpy(record.data() + sizeof(hdr), payload.data(), payload.size());
  return record;
}

struct AppendThreadSpec {
  uint64_t thread_id;
  uint64_t start_sequence;
};

} // namespace

TEST_CASE("file IO mixed aligned/unaligned writes and reads",
          "[integration][read][write][open][setattr]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("io.bin");
    int fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(fd);

    struct stat st{};
    FDBFS_REQUIRE_OK(::fstat(fd, &st));
    REQUIRE(st.st_blksize > 0);
    const size_t bs = static_cast<size_t>(st.st_blksize);

    std::vector<uint8_t> expected;

    const std::vector<uint8_t> simple = {'h', 'e', 'l', 'l', 'o', '\0', 'x'};
    pwrite_all_fd(fd, simple.data(), simple.size(), 0);
    apply_expected_write(expected, 0, simple);
    verify_whole_file(p, expected);

    const auto incompressible1 =
        generate_bytes(bs + 73, BytePattern::Random, 0, 0x1111ull);
    pwrite_all_fd(fd, incompressible1.data(), incompressible1.size(),
                  static_cast<off_t>(bs / 2));
    apply_expected_write(expected, static_cast<off_t>(bs / 2), incompressible1);
    verify_whole_file(p, expected);

    const std::vector<uint8_t> zeros(bs * 2 + 11, 0);
    pwrite_all_fd(fd, zeros.data(), zeros.size(), static_cast<off_t>(bs * 3));
    apply_expected_write(expected, static_cast<off_t>(bs * 3), zeros);
    verify_whole_file(p, expected);

    const auto incompressible2 =
        generate_bytes(bs * 5 + 19, BytePattern::Random, 0, 0x5555ull);
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

    const auto eof_read =
        pread_exact_fd(fd, 64, static_cast<off_t>(expected.size()));
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

    const auto final_payload =
        generate_bytes(bs + 13, BytePattern::Random, 0, 0x9999ull);
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

    struct stat st{};
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
        {"random",
         [](size_t n) {
           return generate_bytes(n, BytePattern::Random, 0, 0x0f00ba11ULL);
         }},
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
    INFO("second pwrite rc=" << wrote
                             << " errno=" << errno_with_message(errno));
    REQUIRE(wrote == static_cast<ssize_t>(block33_patch.size()));

    FDBFS_REQUIRE_OK(::close(fd));
  });
}

TEST_CASE("O_APPEND writes are atomic under concurrency",
          "[integration][read][write][open][file_io][stress]") {
  scenario([&](FdbfsEnv &env) {
    const fs::path p = env.p("o-append-concurrency.bin");

    int prep_fd = ::open(p.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    FDBFS_REQUIRE_NONNEG(prep_fd);

    struct stat st{};
    FDBFS_REQUIRE_OK(::fstat(prep_fd, &st));
    REQUIRE(st.st_blksize > 0);

    const size_t padding_size = 4093;
    const std::vector<uint8_t> padding =
        generate_bytes(padding_size, BytePattern::Random, 0, 0x0a8831ULL);
    write_all_fd(prep_fd, padding.data(), padding.size());
    FDBFS_REQUIRE_OK(::close(prep_fd));

    const unsigned hw = std::thread::hardware_concurrency();
    const size_t thread_count = std::clamp<size_t>(hw == 0 ? 4 : hw, 4, 8);
    const size_t writes_per_thread = 250;
    const size_t max_record_size = std::min<size_t>(
        32768,
        std::max<size_t>(4096, static_cast<size_t>(st.st_blksize) * 3 + 257));
    REQUIRE(max_record_size > sizeof(AppendRecordHeader));
    const size_t max_payload_size =
        max_record_size - sizeof(AppendRecordHeader);

    INFO("thread_count=" << thread_count
                         << " writes_per_thread=" << writes_per_thread
                         << " max_record_size=" << max_record_size);

    std::vector<AppendThreadSpec> specs(thread_count);
    for (size_t i = 0; i < thread_count; i++) {
      specs[i].thread_id = mix64(0x5eed000000000000ULL + i * 0x10001ULL);
      specs[i].start_sequence = mix64(0xabc0000000000000ULL + i * 0x101ULL);
    }

    std::atomic<size_t> ready{0};
    std::atomic<bool> go{false};
    std::atomic<bool> stop{false};
    std::mutex failure_mu;
    std::vector<std::string> failures;

    auto record_failure = [&](std::string msg) {
      std::lock_guard<std::mutex> lk(failure_mu);
      failures.push_back(std::move(msg));
      stop.store(true, std::memory_order_relaxed);
    };

    std::vector<std::thread> threads;
    threads.reserve(thread_count);
    for (size_t idx = 0; idx < thread_count; idx++) {
      threads.emplace_back([&, idx]() {
        const auto spec = specs[idx];
        int fd = ::open(p.c_str(), O_WRONLY | O_APPEND);
        if (fd < 0) {
          record_failure("open failed for writer thread " +
                         std::to_string(idx) + ": " +
                         errno_with_message(errno));
          return;
        }

        ready.fetch_add(1, std::memory_order_relaxed);
        while (!go.load(std::memory_order_acquire) &&
               !stop.load(std::memory_order_relaxed)) {
          ::sched_yield();
        }

        for (size_t i = 0;
             i < writes_per_thread && !stop.load(std::memory_order_relaxed);
             i++) {
          const uint64_t sequence = spec.start_sequence + i;
          const size_t payload_size =
              append_payload_size(spec.thread_id, sequence, max_payload_size);
          std::vector<uint8_t> record =
              make_append_record(spec.thread_id, sequence, payload_size);

          const ssize_t wrote = ::write(fd, record.data(), record.size());
          if (wrote != static_cast<ssize_t>(record.size())) {
            record_failure("write failed for thread " + std::to_string(idx) +
                           " sequence=" + std::to_string(sequence) +
                           ": rc=" + std::to_string(wrote) +
                           " errno=" + errno_with_message(errno));
            break;
          }

          if ((i % 17) == 16) {
            if (::close(fd) != 0) {
              record_failure("close during reopen failed for thread " +
                             std::to_string(idx) + ": " +
                             errno_with_message(errno));
              fd = -1;
              break;
            }
            fd = ::open(p.c_str(), O_WRONLY | O_APPEND);
            if (fd < 0) {
              record_failure("reopen failed for thread " + std::to_string(idx) +
                             ": " + errno_with_message(errno));
              break;
            }
          }

          if ((i % 7) == 3) {
            ::sched_yield();
          }
          if ((i % 29) == 11) {
            const uint64_t delay = mix64(spec.thread_id ^ sequence) % 200;
            if (delay != 0) {
              ::usleep(static_cast<useconds_t>(delay));
            }
          }
        }

        if (fd >= 0 && ::close(fd) != 0) {
          record_failure("final close failed for thread " +
                         std::to_string(idx) + ": " +
                         errno_with_message(errno));
        }
      });
    }

    while (ready.load(std::memory_order_acquire) < thread_count &&
           !stop.load(std::memory_order_relaxed)) {
      ::sched_yield();
    }
    go.store(true, std::memory_order_release);

    for (auto &thread : threads) {
      thread.join();
    }

    INFO("thread_failures=" << failures.size());
    for (const auto &failure : failures) {
      INFO(failure);
    }
    REQUIRE(failures.empty());

    const std::vector<uint8_t> file_bytes = read_file_all(p);
    REQUIRE(file_bytes.size() >= padding.size());
    CHECK(std::equal(padding.begin(), padding.end(), file_bytes.begin()));

    std::unordered_map<uint64_t, size_t> thread_index;
    for (size_t i = 0; i < specs.size(); i++) {
      thread_index.emplace(specs[i].thread_id, i);
    }
    std::vector<std::vector<bool>> seen(
        thread_count, std::vector<bool>(writes_per_thread, false));
    std::vector<std::optional<uint64_t>> last_sequence(thread_count);

    size_t parsed_records = 0;
    size_t offset = padding.size();
    while (offset < file_bytes.size()) {
      INFO("parse_offset=" << offset);
      REQUIRE(file_bytes.size() - offset >= sizeof(AppendRecordHeader));

      AppendRecordHeader hdr{};
      std::memcpy(&hdr, file_bytes.data() + offset, sizeof(hdr));
      offset += sizeof(hdr);

      REQUIRE(hdr.magic == kAppendRecordMagic);
      CHECK(hdr.reserved == 0);
      REQUIRE(hdr.payload_size >= 1);
      REQUIRE(hdr.payload_size <= max_payload_size);
      REQUIRE(hash_append_header(hdr) == hdr.header_hash);

      const auto it = thread_index.find(hdr.thread_id);
      REQUIRE(it != thread_index.end());
      const size_t tidx = it->second;
      const uint64_t start_sequence = specs[tidx].start_sequence;
      REQUIRE(hdr.sequence >= start_sequence);
      const uint64_t sequence_index_u64 = hdr.sequence - start_sequence;
      REQUIRE(sequence_index_u64 < writes_per_thread);
      const size_t sequence_index = static_cast<size_t>(sequence_index_u64);

      CHECK(!seen[tidx][sequence_index]);
      seen[tidx][sequence_index] = true;

      if (last_sequence[tidx].has_value()) {
        CHECK(hdr.sequence > *last_sequence[tidx]);
      }
      last_sequence[tidx] = hdr.sequence;

      REQUIRE(file_bytes.size() - offset >= hdr.payload_size);
      std::span<const uint8_t> payload(file_bytes.data() + offset,
                                       hdr.payload_size);
      offset += hdr.payload_size;

      CHECK(fnv1a_hash(payload) == hdr.payload_hash);

      const std::vector<uint8_t> expected_payload =
          append_payload(hdr.thread_id, hdr.sequence, hdr.payload_size);
      CHECK(std::equal(expected_payload.begin(), expected_payload.end(),
                       payload.begin(), payload.end()));

      parsed_records++;
    }

    const size_t expected_records = thread_count * writes_per_thread;
    CHECK(parsed_records == expected_records);

    for (size_t tidx = 0; tidx < thread_count; tidx++) {
      const size_t seen_count = static_cast<size_t>(
          std::count(seen[tidx].begin(), seen[tidx].end(), true));
      INFO("thread_index=" << tidx << " thread_id=" << specs[tidx].thread_id
                           << " seen_count=" << seen_count);
      CHECK(seen_count == writes_per_thread);
    }
  });
}
