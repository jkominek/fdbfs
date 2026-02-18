#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <errno.h>
#include <limits>
#include <random>
#include <span>
#include <string>
#include <vector>

#include "liveness.h"
#include "util.h"

// Test-local definitions for globals normally owned by fs main/liveness.
FDBDatabase *database = nullptr;
uint8_t BLOCKBITS = 13;
uint32_t BLOCKSIZE = 1u << 13;
std::vector<uint8_t> pid(PID_LENGTH, 0x42);
extern uint64_t next_lookup_generation;

namespace {

std::vector<std::vector<uint8_t>> prefix_cases() {
  std::vector<std::vector<uint8_t>> out;
  out.push_back({});
  out.push_back({0x00});
  out.push_back({0xfe});
  out.push_back({'F', 'S'});

  std::mt19937_64 rng(0x9ad42f3914ULL);
  std::uniform_int_distribution<int> len_dist(3, 6);
  std::uniform_int_distribution<int> byte_dist(0, 254);
  std::vector<uint8_t> random_prefix(static_cast<size_t>(len_dist(rng)));
  for (auto &b : random_prefix) {
    b = static_cast<uint8_t>(byte_dist(rng));
  }
  out.push_back(std::move(random_prefix));
  return out;
}

std::vector<fuse_ino_t> inode_cases() {
  std::vector<fuse_ino_t> out = {
      0ULL,
      1ULL,
      0x0102030405060708ULL,
      0x1111222233334444ULL,
      std::numeric_limits<fuse_ino_t>::max() - 1,
      std::numeric_limits<fuse_ino_t>::max(),
  };
  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
  return out;
}

std::vector<uint64_t> block_cases() {
  std::vector<uint64_t> out = {
      0ULL,
      1ULL,
      2ULL,
      17ULL,
      0x10000ULL,
      std::numeric_limits<uint64_t>::max() - 1,
      std::numeric_limits<uint64_t>::max(),
  };
  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
  return out;
}

bool range_contains(const range_keys &r, const std::vector<uint8_t> &k) {
  return (r.first <= k) && (k < r.second);
}

bool ranges_overlap(const range_keys &a, const range_keys &b) {
  return (a.first < b.second) && (b.first < a.second);
}

std::string bytes_to_hex(const std::vector<uint8_t> &v) {
  static constexpr char hex[] = "0123456789abcdef";
  std::string out;
  out.reserve(v.size() * 2);
  for (uint8_t b : v) {
    out.push_back(hex[(b >> 4) & 0x0f]);
    out.push_back(hex[b & 0x0f]);
  }
  return out;
}

} // namespace

TEST_CASE("prefix_range_end ordering and tightness", "[pure][helpers][range]") {
  const std::vector<std::vector<uint8_t>> direct_prefixes = {
      {0x00},
      {0x01},
      {0xfe},
      {0x00, 0xff},
      {0xff, 0x00},
      {0x10, 0x20, 0x30},
      {0x10, 0xff, 0x40},
      {'F', 'S'},
  };

  for (const auto &p : direct_prefixes) {
    INFO("prefix_hex=" << bytes_to_hex(p));
    const auto e = prefix_range_end(p);
    CHECK(p < e);
  }

  const auto inodes = inode_cases();
  const auto blocks = block_cases();
  const std::vector<uint64_t> op_ids = {
      1ULL, 2ULL, 17ULL, 1024ULL, std::numeric_limits<uint64_t>::max() - 1};

  for (const auto &prefix : prefix_cases()) {
    INFO("key_prefix_hex=" << bytes_to_hex(prefix));
    key_prefix = prefix;

    for (size_t i = 0; i + 1 < inodes.size(); i++) {
      if (inodes[i] == std::numeric_limits<fuse_ino_t>::max()) {
        continue;
      }
      const auto k = pack_inode_key(inodes[i]);
      const auto knext = pack_inode_key(inodes[i + 1]);
      CHECK(knext >= prefix_range_end(k));
    }

    for (size_t i = 0; i + 1 < blocks.size(); i++) {
      if (blocks[i] == std::numeric_limits<uint64_t>::max()) {
        continue;
      }
      const auto k = pack_fileblock_key(1, blocks[i]);
      const auto knext = pack_fileblock_key(1, blocks[i + 1]);
      CHECK(knext >= prefix_range_end(k));
    }

    for (size_t i = 0; i + 1 < op_ids.size(); i++) {
      const auto k = pack_oplog_key(pid, op_ids[i]);
      const auto knext = pack_oplog_key(pid, op_ids[i + 1]);
      CHECK(knext >= prefix_range_end(k));
    }
  }
}

TEST_CASE("pack helpers: monotonic numeric ordering", "[pure][helpers][pack]") {
  const auto inodes = inode_cases();
  const auto blocks = block_cases();
  const std::vector<uint64_t> op_ids = {
      1ULL, 2ULL, 17ULL, 1024ULL, std::numeric_limits<uint64_t>::max() - 1};

  for (const auto &prefix : prefix_cases()) {
    INFO("key_prefix_hex=" << bytes_to_hex(prefix));
    key_prefix = prefix;

    for (size_t i = 0; i + 1 < inodes.size(); i++) {
      const fuse_ino_t a = inodes[i];
      const fuse_ino_t b = inodes[i + 1];
      REQUIRE(a < b);

      CHECK(pack_inode_key(a) < pack_inode_key(b));
      CHECK(pack_inode_key(b) > pack_inode_key(a));

      CHECK(pack_garbage_key(a) < pack_garbage_key(b));
      CHECK(pack_garbage_key(b) > pack_garbage_key(a));

      CHECK(pack_dentry_key(a, "x") < pack_dentry_key(b, "x"));
      CHECK(pack_dentry_key(b, "x") > pack_dentry_key(a, "x"));

      CHECK(pack_xattr_key(a, "x") < pack_xattr_key(b, "x"));
      CHECK(pack_xattr_key(b, "x") > pack_xattr_key(a, "x"));

      CHECK(pack_xattr_data_key(a, "x") < pack_xattr_data_key(b, "x"));
      CHECK(pack_xattr_data_key(b, "x") > pack_xattr_data_key(a, "x"));

      CHECK(pack_inode_use_key(a) < pack_inode_use_key(b));
      CHECK(pack_inode_use_key(b) > pack_inode_use_key(a));
    }

    const fuse_ino_t ino = 1;
    for (size_t i = 0; i + 1 < blocks.size(); i++) {
      const uint64_t a = blocks[i];
      const uint64_t b = blocks[i + 1];
      REQUIRE(a < b);
      CHECK(pack_fileblock_key(ino, a) < pack_fileblock_key(ino, b));
      CHECK(pack_fileblock_key(ino, b) > pack_fileblock_key(ino, a));
    }

    for (size_t i = 0; i + 1 < op_ids.size(); i++) {
      const uint64_t a = op_ids[i];
      const uint64_t b = op_ids[i + 1];
      REQUIRE(a < b);
      CHECK(pack_oplog_key(pid, a) < pack_oplog_key(pid, b));
      CHECK(pack_oplog_key(pid, b) > pack_oplog_key(pid, a));
    }
  }
}

TEST_CASE("pack range helpers: start-stop and containment", "[pure][helpers][range]") {
  const auto inodes = inode_cases();
  const auto blocks = block_cases();
  const std::vector<std::vector<uint8_t>> pid_records = {
      {},
      {0x00},
      {0x11, 0x22, 0x33},
      std::vector<uint8_t>(PID_LENGTH, 0xaa),
  };

  for (const auto &prefix : prefix_cases()) {
    INFO("key_prefix_hex=" << bytes_to_hex(prefix));
    key_prefix = prefix;

    const auto pid_space = pack_pid_subspace_range();
    CHECK(pid_space.first < pid_space.second);

    const auto gc_space = pack_garbage_subspace_range();
    CHECK(gc_space.first < gc_space.second);

    for (const auto &record_pid : pid_records) {
      const auto r = pack_pid_record_range(record_pid);
      CHECK(r.first < r.second);
      CHECK(range_contains(r, pack_pid_key(record_pid)));
    }

    const auto oplog_space = pack_oplog_subspace_range(pid);
    CHECK(oplog_space.first < oplog_space.second);
    CHECK(range_contains(oplog_space, pack_oplog_key(pid, 1)));

    const auto oplog_span = pack_local_oplog_span_range(1, 5);
    CHECK(oplog_span.first < oplog_span.second);
    CHECK(range_contains(oplog_span, pack_oplog_key(pid, 1)));
    CHECK(range_contains(oplog_span, pack_oplog_key(pid, 4)));
    CHECK(!range_contains(oplog_span, pack_oplog_key(pid, 5)));

    for (fuse_ino_t ino : inodes) {
      const auto inode_r = pack_inode_subspace_range(ino);
      CHECK(inode_r.first < inode_r.second);
      CHECK(range_contains(inode_r, pack_inode_key(ino)));
      CHECK(range_contains(inode_r, pack_inode_use_key(ino)));

      const auto use_r = pack_inode_use_subspace_range(ino);
      CHECK(use_r.first < use_r.second);
      CHECK(range_contains(use_r, pack_inode_use_key(ino)));

      const auto meta_use_r = pack_inode_metadata_and_use_range(ino);
      CHECK(meta_use_r.first < meta_use_r.second);
      CHECK(range_contains(meta_use_r, pack_inode_key(ino)));
      CHECK(range_contains(meta_use_r, pack_inode_use_key(ino)));

      const auto dentry_r = pack_dentry_subspace_range(ino);
      CHECK(dentry_r.first < dentry_r.second);
      CHECK(range_contains(dentry_r, pack_dentry_key(ino, "name")));

      const auto xnode_r = pack_xattr_node_subspace_range(ino);
      CHECK(xnode_r.first < xnode_r.second);
      CHECK(range_contains(xnode_r, pack_xattr_key(ino, "user.key")));

      const auto xdata_r = pack_xattr_data_subspace_range(ino);
      CHECK(xdata_r.first < xdata_r.second);
      CHECK(range_contains(xdata_r, pack_xattr_data_key(ino, "user.key")));
    }

    for (uint64_t block : blocks) {
      const auto one = pack_fileblock_single_range(1, block);
      CHECK(one.first < one.second);
      CHECK(range_contains(one, pack_fileblock_key(1, block)));
    }
  }
}

TEST_CASE("pack ranges: ordering and non-overlap properties",
          "[pure][helpers][range]") {
  const auto inodes = inode_cases();
  const auto blocks = block_cases();
  for (const auto &prefix : prefix_cases()) {
    INFO("key_prefix_hex=" << bytes_to_hex(prefix));
    key_prefix = prefix;

    for (size_t i = 0; i + 1 < inodes.size(); i++) {
      const fuse_ino_t a = inodes[i];
      const fuse_ino_t b = inodes[i + 1];
      REQUIRE(a < b);

      const auto ia = pack_inode_subspace_range(a);
      const auto ib = pack_inode_subspace_range(b);
      CHECK(ia.second <= ib.first);

      const auto da = pack_dentry_subspace_range(a);
      const auto db = pack_dentry_subspace_range(b);
      CHECK(da.second <= db.first);

      const auto xa = pack_xattr_node_subspace_range(a);
      const auto xb = pack_xattr_node_subspace_range(b);
      CHECK(xa.second <= xb.first);

      const auto xda = pack_xattr_data_subspace_range(a);
      const auto xdb = pack_xattr_data_subspace_range(b);
      CHECK(xda.second <= xdb.first);

      const auto ua = pack_inode_use_subspace_range(a);
      const auto ub = pack_inode_use_subspace_range(b);
      CHECK(ua.second <= ub.first);
    }

    // Non-overlap checks for top-level inode-parameterized spaces on random inodes.
    std::mt19937_64 rng(0x7c00de5ULL);
    std::uniform_int_distribution<uint64_t> ino_dist(
        2ULL, std::numeric_limits<uint64_t>::max() - 2ULL);
    std::vector<fuse_ino_t> random_inodes;
    for (int i = 0; i < 24; i++) {
      random_inodes.push_back(static_cast<fuse_ino_t>(ino_dist(rng)));
    }
    std::sort(random_inodes.begin(), random_inodes.end());
    random_inodes.erase(std::unique(random_inodes.begin(), random_inodes.end()),
                        random_inodes.end());

    auto check_nonoverlap_by_ino = [&](auto make_range) {
      std::vector<range_keys> ranges;
      ranges.reserve(random_inodes.size());
      for (fuse_ino_t ino : random_inodes) {
        ranges.push_back(make_range(ino));
      }
      for (size_t i = 0; i < ranges.size(); i++) {
        for (size_t j = i + 1; j < ranges.size(); j++) {
          CHECK(!ranges_overlap(ranges[i], ranges[j]));
        }
      }
    };

    check_nonoverlap_by_ino(
        [&](fuse_ino_t ino) { return pack_inode_subspace_range(ino); });
    check_nonoverlap_by_ino(
        [&](fuse_ino_t ino) { return pack_fileblock_span_range(ino, 0, UINT64_MAX); });
    check_nonoverlap_by_ino(
        [&](fuse_ino_t ino) { return pack_dentry_subspace_range(ino); });
    check_nonoverlap_by_ino(
        [&](fuse_ino_t ino) { return pack_xattr_node_subspace_range(ino); });
    check_nonoverlap_by_ino(
        [&](fuse_ino_t ino) { return pack_xattr_data_subspace_range(ino); });

    // Cross-family non-overlap checks for selected inodes.
    const std::vector<fuse_ino_t> selected_inodes = {
        0ULL,
        1ULL,
        0x0102030405060708ULL,
        std::numeric_limits<fuse_ino_t>::max() - 1,
        std::numeric_limits<fuse_ino_t>::max(),
    };
    for (const fuse_ino_t ino : selected_inodes) {
      INFO("cross_family_ino=" << ino);
      const std::vector<range_keys> family_ranges = {
          pack_inode_subspace_range(ino),
          pack_fileblock_span_range(ino, 0, UINT64_MAX),
          pack_dentry_subspace_range(ino),
          pack_xattr_node_subspace_range(ino),
          pack_xattr_data_subspace_range(ino),
      };
      for (size_t i = 0; i < family_ranges.size(); i++) {
        for (size_t j = i + 1; j < family_ranges.size(); j++) {
          CHECK(!ranges_overlap(family_ranges[i], family_ranges[j]));
        }
      }
    }

    // Span ordering: if a < b < c then [a,b] should end at/before [b+1,c] starts.
    for (size_t i = 0; i + 2 < blocks.size(); i++) {
      const uint64_t a = blocks[i];
      const uint64_t b = blocks[i + 1];
      const uint64_t c = blocks[i + 2];
      REQUIRE(a < b);
      REQUIRE(b < c);

      const auto left = pack_fileblock_span_range(1, a, b);
      const auto right = pack_fileblock_span_range(1, b + 1, c);
      CHECK(left.second <= right.first);
    }
  }
}

TEST_CASE("pack name helpers preserve lexicographic name ordering",
          "[pure][helpers][pack]") {
  const std::vector<std::string> names = {
      "",
      "a",
      "aa",
      "ab",
      "b",
      std::string("\x00", 1),
      std::string("\x01", 1),
      std::string("\x01\x00", 2),
      std::string("\xff", 1),
      std::string("z\x00z", 3),
  };

  for (const auto &prefix : prefix_cases()) {
    INFO("key_prefix_hex=" << bytes_to_hex(prefix));
    key_prefix = prefix;

    for (size_t i = 0; i < names.size(); i++) {
      for (size_t j = 0; j < names.size(); j++) {
        const auto &a = names[i];
        const auto &b = names[j];

        const auto da = pack_dentry_key(1, a);
        const auto db = pack_dentry_key(1, b);
        if (a < b) {
          CHECK(da < db);
        } else if (a > b) {
          CHECK(da > db);
        } else {
          CHECK(da == db);
        }

        const auto xa = pack_xattr_key(1, a);
        const auto xb = pack_xattr_key(1, b);
        if (a < b) {
          CHECK(xa < xb);
        } else if (a > b) {
          CHECK(xa > xb);
        } else {
          CHECK(xa == xb);
        }

        const auto xda = pack_xattr_data_key(1, a);
        const auto xdb = pack_xattr_data_key(1, b);
        if (a < b) {
          CHECK(xda < xdb);
        } else if (a > b) {
          CHECK(xda > xdb);
        } else {
          CHECK(xda == xdb);
        }
      }
    }
  }
}

TEST_CASE("inode/stat packing helpers preserve expected fields",
          "[pure][helpers][stat]") {
  BLOCKBITS = 13;
  BLOCKSIZE = 1u << BLOCKBITS;

  SECTION("pack_inode_record_into_stat defaults and field mapping") {
    INodeRecord inode;
    inode.set_inode(1234);
    inode.set_mode(0640);
    inode.set_type(ft_regular);
    inode.set_nlinks(2);
    inode.mutable_atime()->set_sec(10);
    inode.mutable_atime()->set_nsec(11);
    inode.mutable_mtime()->set_sec(20);
    inode.mutable_mtime()->set_nsec(21);
    inode.mutable_ctime()->set_sec(30);
    inode.mutable_ctime()->set_nsec(31);

    struct stat st {};
    pack_inode_record_into_stat(inode, st);

    CHECK(st.st_ino == 1234);
    CHECK(st.st_mode == (0640 | S_IFREG));
    CHECK(st.st_nlink == 2);
    CHECK(st.st_uid == 0);
    CHECK(st.st_gid == 0);
    CHECK(st.st_size == 0);
    CHECK(st.st_atim.tv_sec == 10);
    CHECK(st.st_atim.tv_nsec == 11);
    CHECK(st.st_mtim.tv_sec == 20);
    CHECK(st.st_mtim.tv_nsec == 21);
    CHECK(st.st_ctim.tv_sec == 30);
    CHECK(st.st_ctim.tv_nsec == 31);
    CHECK(st.st_blksize == static_cast<decltype(st.st_blksize)>(BLOCKSIZE));
    CHECK(st.st_blocks == 1);
  }

  SECTION("pack_stat_into_stat_record and unpack roundtrip") {
    struct stat in {};
    in.st_ino = 99;
    in.st_dev = 7;
    in.st_mode = S_IFDIR | 0755;
    in.st_nlink = 5;
    in.st_uid = 1001;
    in.st_gid = 1002;
    in.st_size = 123456;
    in.st_atim.tv_sec = 100;
    in.st_atim.tv_nsec = 101;
    in.st_mtim.tv_sec = 200;
    in.st_mtim.tv_nsec = 201;
    in.st_ctim.tv_sec = 300;
    in.st_ctim.tv_nsec = 301;
    in.st_blksize = 4096;
    in.st_blocks = 241;

    const StatRecord rec = pack_stat_into_stat_record(in);
    struct stat out {};
    unpack_stat_record_into_stat(rec, out);

    CHECK(out.st_ino == in.st_ino);
    CHECK(out.st_dev == in.st_dev);
    CHECK(out.st_mode == in.st_mode);
    CHECK(out.st_nlink == in.st_nlink);
    CHECK(out.st_uid == in.st_uid);
    CHECK(out.st_gid == in.st_gid);
    CHECK(out.st_size == in.st_size);
    CHECK(out.st_atim.tv_sec == in.st_atim.tv_sec);
    CHECK(out.st_atim.tv_nsec == in.st_atim.tv_nsec);
    CHECK(out.st_mtim.tv_sec == in.st_mtim.tv_sec);
    CHECK(out.st_mtim.tv_nsec == in.st_mtim.tv_nsec);
    CHECK(out.st_ctim.tv_sec == in.st_ctim.tv_sec);
    CHECK(out.st_ctim.tv_nsec == in.st_ctim.tv_nsec);
    CHECK(out.st_blksize == in.st_blksize);
    CHECK(out.st_blocks == in.st_blocks);
  }
}

TEST_CASE("inode time update helpers implement cascade semantics",
          "[pure][helpers][time]") {
  INodeRecord inode;
  inode.mutable_atime()->set_sec(1);
  inode.mutable_atime()->set_nsec(2);
  inode.mutable_mtime()->set_sec(3);
  inode.mutable_mtime()->set_nsec(4);
  inode.mutable_ctime()->set_sec(5);
  inode.mutable_ctime()->set_nsec(6);

  const timespec t1{10, 11};
  update_atime(&inode, &t1);
  CHECK(inode.atime().sec() == 10);
  CHECK(inode.atime().nsec() == 11);
  CHECK(inode.mtime().sec() == 3);
  CHECK(inode.mtime().nsec() == 4);
  CHECK(inode.ctime().sec() == 5);
  CHECK(inode.ctime().nsec() == 6);

  const timespec t2{20, 21};
  update_ctime(&inode, &t2);
  CHECK(inode.ctime().sec() == 20);
  CHECK(inode.ctime().nsec() == 21);
  CHECK(inode.atime().sec() == 20);
  CHECK(inode.atime().nsec() == 21);
  CHECK(inode.mtime().sec() == 3);
  CHECK(inode.mtime().nsec() == 4);

  const timespec t3{30, 31};
  update_mtime(&inode, &t3);
  CHECK(inode.mtime().sec() == 30);
  CHECK(inode.mtime().nsec() == 31);
  CHECK(inode.ctime().sec() == 30);
  CHECK(inode.ctime().nsec() == 31);
  CHECK(inode.atime().sec() == 30);
  CHECK(inode.atime().nsec() == 31);
}

TEST_CASE("offset_size_to_range_keys maps offsets to expected block spans",
          "[pure][helpers][range]") {
  key_prefix = {'F', 'S'};
  BLOCKBITS = 13;
  BLOCKSIZE = 1u << BLOCKBITS;

  SECTION("single-byte at start of file") {
    const auto got = offset_size_to_range_keys(7, 0, 1);
    const auto expected = pack_fileblock_span_range(7, 0, 0);
    CHECK(got.first == expected.first);
    CHECK(got.second == expected.second);
  }

  SECTION("crosses block boundary by one byte") {
    const auto got = offset_size_to_range_keys(7, BLOCKSIZE - 1, 2);
    const auto expected = pack_fileblock_span_range(7, 0, 1);
    CHECK(got.first == expected.first);
    CHECK(got.second == expected.second);
  }

  SECTION("full aligned block") {
    const auto got = offset_size_to_range_keys(7, BLOCKSIZE, BLOCKSIZE);
    const auto expected = pack_fileblock_span_range(7, 1, 1);
    CHECK(got.first == expected.first);
    CHECK(got.second == expected.second);
  }
}

TEST_CASE("decode_block handles plain and error paths",
          "[pure][helpers][decode]") {
  key_prefix = {'F', 'S'};
  fileblock_key_length = static_cast<int>(pack_fileblock_key(1, 0).size());

  SECTION("plain key decodes bounded slice") {
    auto key = pack_fileblock_key(11, 3);
    std::vector<uint8_t> value = {10, 11, 12, 13, 14, 15};
    FDBKeyValue kv{};
    kv.key = key.data();
    kv.key_length = static_cast<int>(key.size());
    kv.value = value.data();
    kv.value_length = static_cast<int>(value.size());

    std::vector<uint8_t> out(8, 0);
    auto decoded = decode_block(&kv, 2, std::span<uint8_t>(out.data(), out.size()), 10);
    REQUIRE(decoded.has_value());
    CHECK(decoded.value() == 4);
    CHECK(out[0] == 12);
    CHECK(out[1] == 13);
    CHECK(out[2] == 14);
    CHECK(out[3] == 15);
  }

  SECTION("plain key with offset beyond value returns zero") {
    auto key = pack_fileblock_key(11, 3);
    std::vector<uint8_t> value = {1, 2, 3};
    FDBKeyValue kv{};
    kv.key = key.data();
    kv.key_length = static_cast<int>(key.size());
    kv.value = value.data();
    kv.value_length = static_cast<int>(value.size());

    std::vector<uint8_t> out(4, 0xaa);
    auto decoded = decode_block(&kv, 100, std::span<uint8_t>(out.data(), out.size()), 4);
    REQUIRE(decoded.has_value());
    CHECK(decoded.value() == 0);
    CHECK(out[0] == 0xaa);
  }

  SECTION("negative offset is invalid") {
    auto key = pack_fileblock_key(11, 3);
    std::vector<uint8_t> value = {1};
    FDBKeyValue kv{};
    kv.key = key.data();
    kv.key_length = static_cast<int>(key.size());
    kv.value = value.data();
    kv.value_length = static_cast<int>(value.size());

    std::vector<uint8_t> out(4, 0);
    auto decoded = decode_block(&kv, -1, std::span<uint8_t>(out.data(), out.size()), 4);
    REQUIRE(!decoded.has_value());
    CHECK(decoded.error() == EINVAL);
  }

  SECTION("unknown special block type returns EIO") {
    auto key = pack_fileblock_key(11, 3);
    key.push_back('?');
    key.push_back(0x01);
    key.push_back(0x7f);
    std::vector<uint8_t> value = {1, 2, 3};
    FDBKeyValue kv{};
    kv.key = key.data();
    kv.key_length = static_cast<int>(key.size());
    kv.value = value.data();
    kv.value_length = static_cast<int>(value.size());

    std::vector<uint8_t> out(4, 0);
    auto decoded = decode_block(&kv, 0, std::span<uint8_t>(out.data(), out.size()), 4);
    REQUIRE(!decoded.has_value());
    CHECK(decoded.error() == EIO);
  }
}

TEST_CASE("lookup count helpers maintain count and generation behavior",
          "[pure][helpers][lookup]") {
  {
    std::lock_guard<std::mutex> guard(lookup_counts_mutex);
    lookup_counts.clear();
  }
  next_lookup_generation = 1;

  CHECK(!lookup_count_nonzero(100));

  const auto g1 = increment_lookup_count(100);
  REQUIRE(g1.has_value());
  CHECK(g1.value() == 1);
  CHECK(lookup_count_nonzero(100));

  const auto g1b = increment_lookup_count(100);
  CHECK(!g1b.has_value());
  CHECK(lookup_count_nonzero(100));

  const auto d1 = decrement_lookup_count(100, 1);
  CHECK(!d1.has_value());
  CHECK(lookup_count_nonzero(100));

  const auto d2 = decrement_lookup_count(100, 1);
  REQUIRE(d2.has_value());
  CHECK(d2.value() == 1);
  CHECK(!lookup_count_nonzero(100));

  const auto d_missing = decrement_lookup_count(100, 1);
  CHECK(!d_missing.has_value());

  const auto g2 = increment_lookup_count(100);
  REQUIRE(g2.has_value());
  CHECK(g2.value() == 2);

  const auto d_over = decrement_lookup_count(100, 999);
  REQUIRE(d_over.has_value());
  CHECK(d_over.value() == 2);
  CHECK(!lookup_count_nonzero(100));
}
