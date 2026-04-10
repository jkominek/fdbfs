#pragma once

#include <CLI11.hpp>

#include <memory>
#include <stdexcept>
#include <string>

#include "generic/fdb_service.h"
#include "generic/fdbfs_runtime.h"
#include "generic/util.h"
#include "values.pb.h"

struct CommonCliOptions {
  std::string conffile;
  std::string tenant;
  std::string keyprefix;
  bool verbose = false;
};

inline void add_common_cli_options(CLI::App &app, CommonCliOptions &options) {
  app.add_option("-C", options.conffile, "FoundationDB cluster file")
      ->envname("FDB_CLUSTER_FILE")
      ->option_text("CONFFILE");

  app.add_option("-T", options.tenant, "FoundationDB tenant")
      ->envname("FDB_TENANT")
      ->option_text("TENANT");

  app.add_flag("-v", options.verbose, "Enable verbose output");

  app.add_option("KEYPREFIX", options.keyprefix,
                 "Key prefix for all filesystem keys")
      ->required();
}

inline void initialize_common_fdbfs_state(const CommonCliOptions &options) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  if (options.keyprefix.empty()) {
    throw std::runtime_error("KEYPREFIX must be non-empty");
  }
  if (!options.tenant.empty()) {
    throw std::runtime_error(
        "FoundationDB tenants are not supported by this tool yet");
  }

  key_prefix.assign(options.keyprefix.begin(), options.keyprefix.end());
  BLOCKBITS = 13;
  BLOCKSIZE = 1u << BLOCKBITS;
  inode_key_length = pack_inode_key(0).size();
  fileblock_prefix_length = inode_key_length;
  fileblock_key_length = pack_fileblock_key(0, 0).size();
  dirent_prefix_length = pack_dentry_key(0, "").size();
  lookup_counts.reserve(128);
  shut_it_down_forever = false;
}

class CommonCliRuntimeGuard {
public:
  explicit CommonCliRuntimeGuard(const CommonCliOptions &options) {
    initialize_common_fdbfs_state(options);

    runtime_.add_persistent<FdbService>(
        [cluster_file = options.conffile]() -> std::unique_ptr<FdbService> {
          return std::make_unique<FdbService>(
              cluster_file.empty() ? nullptr : cluster_file.c_str(), false);
        });
    g_fdbfs_runtime = &runtime_;
    runtime_.start_persistent();
  }

  ~CommonCliRuntimeGuard() { g_fdbfs_runtime = nullptr; }

  CommonCliRuntimeGuard(const CommonCliRuntimeGuard &) = delete;
  CommonCliRuntimeGuard &operator=(const CommonCliRuntimeGuard &) = delete;
  CommonCliRuntimeGuard(CommonCliRuntimeGuard &&) = delete;
  CommonCliRuntimeGuard &operator=(CommonCliRuntimeGuard &&) = delete;

  [[nodiscard]] FdbfsRuntime &runtime() { return runtime_; }

private:
  FdbfsRuntime runtime_;
};
