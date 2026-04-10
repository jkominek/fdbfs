#pragma once

#include <CLI11.hpp>

#include <string>

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
