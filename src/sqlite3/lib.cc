#define FDB_API_VERSION 730

#include <cstddef>
#include <cstdint>
#include <memory>

#include <sqlite3.h>

#include "fdb_service.h"
#include "fdbfs_runtime.h"
#include "garbage_collector.h"
#include "liveness.h"
#include "sqlite_ops.h"
#include "util_locks.h"

uint8_t BLOCKBITS = 13;
uint32_t BLOCKSIZE = 1u << 13;

namespace {

std::unique_ptr<FdbfsRuntime> g_sqlite3_runtime;
sqlite3_vfs g_fdbfs_vfs = {
    3,                         // iVersion
    sizeof(struct fdbfs_file), // szOsFile
    255,                       // mxPathname
    nullptr,                   // pNext
    "fdbfs",                   // zName
    nullptr,                   // pAppData
    fdbfs_sqlite3_xOpen,       // xOpen
    fdbfs_sqlite3_xDelete,     // xDelete
    fdbfs_sqlite3_xAccess,     // xAccess
    fdbfs_sqlite3_xFullPathname, // xFullPathname
    fdbfs_sqlite3_xDlOpen,     // xDlOpen
    fdbfs_sqlite3_xDlError,    // xDlError
    fdbfs_sqlite3_xDlSym,      // xDlSym
    fdbfs_sqlite3_xDlClose,    // xDlClose
    fdbfs_sqlite3_xRandomness, // xRandomness
    fdbfs_sqlite3_xSleep,      // xSleep
    fdbfs_sqlite3_xCurrentTime, // xCurrentTime
    fdbfs_sqlite3_xGetLastError, // xGetLastError
    fdbfs_sqlite3_xCurrentTimeInt64, // xCurrentTimeInt64
    // we don't provide the following
    nullptr, // xSetSystemCall
    nullptr, // xGetSystemCall
    nullptr, // xNextSystemCall
};

} // namespace

extern "C" __attribute__((constructor)) void fdbfs_sqlite3_library_init() {
  if (g_sqlite3_runtime) {
    return;
  }

  auto runtime = std::make_unique<FdbfsRuntime>();
  runtime->add_persistent<FdbService>(
      []() { return std::make_unique<FdbService>(false); });
  runtime->add_restartable<LivenessService>(
      []() { return std::make_unique<LivenessService>([]() {}); });
  runtime->add_restartable<GarbageCollectorService>(
      []() { return std::make_unique<GarbageCollectorService>(); });
  runtime->add_restartable<LockManagerService>(
      []() { return std::make_unique<LockManagerService>(); });
  runtime->start_all();
  g_sqlite3_runtime = std::move(runtime);

  sqlite3_vfs_register(&g_fdbfs_vfs, 0);
}

extern "C" __attribute__((destructor)) void fdbfs_sqlite3_library_finish() {
  sqlite3_vfs_unregister(&g_fdbfs_vfs);
  g_sqlite3_runtime.reset();
}
