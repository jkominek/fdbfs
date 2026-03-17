#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <CLI11.hpp>

#include "common_cli.h"
#include "values.pb.h"

int main(int argc, char **argv) {
  CLI::App app{"mkfs.fdbfs"};
  CommonCliOptions options;
  add_common_cli_options(app, options);
  CLI11_PARSE(app, argc, argv);
  return 0;
}
