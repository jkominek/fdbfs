#define FDB_API_VERSION 730

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string_view>

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "fdb_service.h"
#include "fdbfs_runtime.h"
#include "garbage_collector.h"
#include "liveness.h"
#include "sqlite_ops.h"
#include "util.h"
#include "util_locks.h"
#include "values.pb.h"

uint8_t BLOCKBITS = 13;
uint32_t BLOCKSIZE = 1u << 13;

namespace {

std::once_flag g_sqlite3_runtime_once;
bool g_sqlite3_runtime_start_failed = false;
sqlite3_vfs g_fdbfs_vfs = {
    3,                               // iVersion
    sizeof(struct fdbfs_file),       // szOsFile
    8192,                            // mxPathname
    nullptr,                         // pNext
    "fdbfs",                         // zName
    nullptr,                         // pAppData
    fdbfs_sqlite3_xOpen,             // xOpen
    fdbfs_sqlite3_xDelete,           // xDelete
    fdbfs_sqlite3_xAccess,           // xAccess
    fdbfs_sqlite3_xFullPathname,     // xFullPathname
    fdbfs_sqlite3_xDlOpen,           // xDlOpen
    fdbfs_sqlite3_xDlError,          // xDlError
    fdbfs_sqlite3_xDlSym,            // xDlSym
    fdbfs_sqlite3_xDlClose,          // xDlClose
    fdbfs_sqlite3_xRandomness,       // xRandomness
    fdbfs_sqlite3_xSleep,            // xSleep
    fdbfs_sqlite3_xCurrentTime,      // xCurrentTime
    fdbfs_sqlite3_xGetLastError,     // xGetLastError
    fdbfs_sqlite3_xCurrentTimeInt64, // xCurrentTimeInt64
    // we don't provide the following
    nullptr, // xSetSystemCall
    nullptr, // xGetSystemCall
    nullptr, // xNextSystemCall
};

} // namespace

bool fdbfs_sqlite3_ensure_runtime() {
  std::call_once(g_sqlite3_runtime_once, []() {
    try {
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
    } catch (...) {
      g_sqlite3_runtime_start_failed = true;
    }
  });

  return (g_sqlite3_runtime != nullptr) && !g_sqlite3_runtime_start_failed;
}

extern "C" int sqlite3_extension_init(sqlite3 *db, char **pzErrMsg,
                                      const sqlite3_api_routines *pApi) {
  SQLITE_EXTENSION_INIT2(pApi);
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  constexpr std::string_view default_key_prefix = "FS";
  key_prefix.clear();
  key_prefix.insert(key_prefix.end(), default_key_prefix.begin(),
                    default_key_prefix.end());

  inode_key_length = pack_inode_key(0).size();
  fileblock_prefix_length = inode_key_length;
  fileblock_key_length = pack_fileblock_key(0, 0).size();
  dirent_prefix_length = pack_dentry_key(0, "").size();
  lookup_counts.reserve(128);

  // we can't be unloaded because of fdb; it can't be started more than
  // once per process.
  if (fdbfs_sqlite3_ensure_runtime()) {
    sqlite3_vfs_register(&g_fdbfs_vfs, 0);
    return SQLITE_OK_LOAD_PERMANENTLY;
  } else {
    return SQLITE_ERROR;
  }
}

extern "C" __attribute__((destructor)) void fdbfs_sqlite3_library_finish() {
  sqlite3_vfs_unregister(&g_fdbfs_vfs);
  g_sqlite3_runtime.reset();
}
