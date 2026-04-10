#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <CLI11.hpp>

#include <cerrno>
#include <cstdio>
#include <exception>
#include <limits>
#include <stdexcept>
#include <string>

#include "common_cli.h"
#include "generic/util.h"
#include "values.pb.h"

uint8_t BLOCKBITS = 13;
uint32_t BLOCKSIZE = 1u << 13;

namespace {

constexpr fdbfs_ino_t kRootIno = 1;

struct MkfsCliOptions : CommonCliOptions {
  bool force = false;
  std::string root_mode = "0755";
  uint32_t root_uid = 0;
  uint32_t root_gid = 0;
};

range_keys mkfs_keyspace_range() {
  return std::make_pair(key_prefix, prefix_range_end(key_prefix));
}

uint32_t parse_root_mode(const std::string &mode_text) {
  size_t pos = 0;
  unsigned long parsed = 0;
  try {
    parsed = std::stoul(mode_text, &pos, 8);
  } catch (const std::exception &) {
    throw std::runtime_error("invalid root mode: '" + mode_text + "'");
  }

  if ((pos != mode_text.size()) ||
      (parsed >
       static_cast<unsigned long>(std::numeric_limits<uint32_t>::max()))) {
    throw std::runtime_error("invalid root mode: '" + mode_text + "'");
  }
  const uint32_t mode = static_cast<uint32_t>(parsed);
  if ((mode & ~07777u) != 0) {
    throw std::runtime_error("root mode has unsupported bits set: '" +
                             mode_text + "'");
  }
  return mode;
}

bool keyspace_has_data(FDBTransaction *transaction, const range_keys &range) {
  unique_future future = wrap_future(fdb_transaction_get_range(
      transaction,
      FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(range.first.data(), range.first.size()),
      FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(range.second.data(),
                                        range.second.size()),
      1, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0));
  if (const fdb_error_t err = fdb_future_block_until_ready(future.get())) {
    throw err;
  }

  const FDBKeyValue *kvs = nullptr;
  int kvcount = 0;
  fdb_bool_t more = 0;
  if (const fdb_error_t err =
          fdb_future_get_keyvalue_array(future.get(), &kvs, &kvcount, &more)) {
    throw err;
  }

  (void)kvs;
  (void)more;
  return kvcount > 0;
}

void add_full_keyspace_conflicts(FDBTransaction *transaction,
                                 const range_keys &range) {
  if (const fdb_error_t err = fdb_transaction_add_conflict_range(
          transaction, range.first.data(), static_cast<int>(range.first.size()),
          range.second.data(), static_cast<int>(range.second.size()),
          FDB_CONFLICT_RANGE_TYPE_READ)) {
    throw err;
  }
  if (const fdb_error_t err = fdb_transaction_add_conflict_range(
          transaction, range.first.data(), static_cast<int>(range.first.size()),
          range.second.data(), static_cast<int>(range.second.size()),
          FDB_CONFLICT_RANGE_TYPE_WRITE)) {
    throw err;
  }
}

} // namespace

static int run_mkfs(const MkfsCliOptions &options) {
  const uint32_t root_mode = parse_root_mode(options.root_mode);

  const auto result =
      run_sync_transaction<int>([&](FDBTransaction *transaction) {
        const range_keys keyspace_range = mkfs_keyspace_range();
        if (keyspace_has_data(transaction, keyspace_range)) {
          if (!options.force) {
            std::fprintf(stderr,
                         "mkfs.fdbfs: key space for prefix '%s' is not empty; "
                         "rerun with --force to overwrite it\n",
                         options.keyprefix.c_str());
            return 1;
          }
          std::fprintf(
              stderr,
              "mkfs.fdbfs: key space for prefix '%s' is not empty; clearing\n",
              options.keyprefix.c_str());
          fdbfs_transaction_clear_range(transaction, keyspace_range);
        }

        struct timespec format_time{};
        if (clock_gettime(CLOCK_REALTIME, &format_time) != 0) {
          throw std::runtime_error("clock_gettime(CLOCK_REALTIME) failed");
        }

        INodeRecord root;
        root.set_inode(kRootIno);
        root.set_type(ft_directory);
        root.set_nlinks(2);
        root.set_uid(options.root_uid);
        root.set_gid(options.root_gid);
        root.set_mode(root_mode);
        root.set_parentinode(kRootIno);
        root.mutable_atime()->set_sec(format_time.tv_sec);
        root.mutable_atime()->set_nsec(format_time.tv_nsec);
        root.mutable_mtime()->set_sec(format_time.tv_sec);
        root.mutable_mtime()->set_nsec(format_time.tv_nsec);
        root.mutable_ctime()->set_sec(format_time.tv_sec);
        root.mutable_ctime()->set_nsec(format_time.tv_nsec);
        root.mutable_btime()->set_sec(format_time.tv_sec);
        root.mutable_btime()->set_nsec(format_time.tv_nsec);

        if (const auto write_result =
                fdb_set_protobuf(transaction, pack_inode_key(kRootIno), root);
            !write_result.has_value()) {
          throw std::runtime_error("failed to serialize or store root inode");
        }

        add_full_keyspace_conflicts(transaction, keyspace_range);
        return 0;
      });

  if (!result.has_value()) {
    std::fprintf(stderr, "mkfs.fdbfs: FoundationDB error %d: %s\n",
                 result.error(), fdb_get_error(result.error()));
    return 1;
  }

  std::fprintf(
      stderr,
      "mkfs.fdbfs: created filesystem\n"
      "  cluster file: %s\n"
      "  key prefix: %s\n"
      "  root uid: %u\n"
      "  root gid: %u\n"
      "  root mode: %04o\n",
      options.conffile.empty() ? "(default)" : options.conffile.c_str(),
      options.keyprefix.c_str(), options.root_uid, options.root_gid, root_mode);
  return *result;
}

int main(int argc, char **argv) {
  CLI::App app{"mkfs.fdbfs"};
  MkfsCliOptions options;
  add_common_cli_options(app, options);
  app.add_flag("-f,--force", options.force,
               "Overwrite all data in the selected key space");
  app.add_option("--mode", options.root_mode, "Root directory mode in octal")
      ->option_text("MODE");
  app.add_option("--uid", options.root_uid, "Root directory uid")
      ->option_text("UID");
  app.add_option("--gid", options.root_gid, "Root directory gid")
      ->option_text("GID");
  CLI11_PARSE(app, argc, argv);

  try {
    CommonCliRuntimeGuard runtime(options);
    return run_mkfs(options);
  } catch (const std::exception &e) {
    std::fprintf(stderr, "mkfs.fdbfs error: %s\n", e.what());
    return 1;
  }
}
