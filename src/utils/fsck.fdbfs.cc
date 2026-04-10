#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <CLI11.hpp>

#include <cstdio>
#include <exception>

#include "common_cli.h"
#include "generic/util.h"
#include "values.pb.h"

uint8_t BLOCKBITS = 13;
uint32_t BLOCKSIZE = 1u << 13;

struct FsckCliOptions : CommonCliOptions {};

static int run_fsck(const FsckCliOptions &options) {
  (void)options;
  return 0;
}

int main(int argc, char **argv) {
  CLI::App app{"fsck.fdbfs"};
  FsckCliOptions options;
  add_common_cli_options(app, options);
  CLI11_PARSE(app, argc, argv);

  try {
    CommonCliRuntimeGuard runtime(options);
    return run_fsck(options);
  } catch (const std::exception &e) {
    std::fprintf(stderr, "fsck.fdbfs error: %s\n", e.what());
    return 1;
  }
}
