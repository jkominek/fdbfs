#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <cstdio>
#include <cstring>
#include <ctime>
#include <dlfcn.h>
#include <errno.h>
#include <sys/random.h>
#include <unistd.h>

#include "sqlite_ops.h"

namespace {

constexpr sqlite3_int64 kJulianUnixEpochMillis = 210866760000000LL;
constexpr sqlite3_int64 kMicrosPerSecond = 1000000LL;
constexpr sqlite3_int64 kMillisPerSecond = 1000LL;

sqlite3_int64 current_time_millis() {
  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  return kJulianUnixEpochMillis + now.tv_sec * kMillisPerSecond +
         now.tv_nsec / 1000000;
}

} // namespace

// VFS Operations

int fdbfs_sqlite3_xOpen(sqlite3_vfs *, sqlite3_filename, sqlite3_file *file,
                        int, int *out_flags) {
  if (file != nullptr) {
    std::memset(file, 0, sizeof(struct fdbfs_file));
  }
  if (out_flags != nullptr) {
    *out_flags = 0;
  }
  (void)g_fdbfs_io_methods;
  return SQLITE_CANTOPEN;
}

int fdbfs_sqlite3_xDelete(sqlite3_vfs *, const char *, int) {
  return SQLITE_IOERR_DELETE;
}

int fdbfs_sqlite3_xAccess(sqlite3_vfs *, const char *, int, int *result_out) {
  if (result_out != nullptr) {
    *result_out = 0;
  }
  return SQLITE_OK;
}

int fdbfs_sqlite3_xFullPathname(sqlite3_vfs *, const char *zName, int nOut,
                                char *zOut) {
  if (zName == nullptr || zOut == nullptr || nOut <= 0) {
    return SQLITE_CANTOPEN;
  }

  const size_t name_len = std::strlen(zName);
  if (name_len + 1 > static_cast<size_t>(nOut)) {
    return SQLITE_CANTOPEN;
  }

  std::memcpy(zOut, zName, name_len + 1);
  return SQLITE_OK;
}

void *fdbfs_sqlite3_xDlOpen(sqlite3_vfs *, const char *zFilename) {
  if (zFilename == nullptr) {
    return nullptr;
  }
  return dlopen(zFilename, RTLD_NOW | RTLD_LOCAL);
}

void fdbfs_sqlite3_xDlError(sqlite3_vfs *, int nByte, char *zErrMsg) {
  if (zErrMsg == nullptr || nByte <= 0) {
    return;
  }

  const char *message = dlerror();
  if (message == nullptr) {
    zErrMsg[0] = '\0';
    return;
  }
  std::snprintf(zErrMsg, static_cast<size_t>(nByte), "%s", message);
}

void (*fdbfs_sqlite3_xDlSym(sqlite3_vfs *, void *handle,
                            const char *zSymbol))(void) {
  if (handle == nullptr || zSymbol == nullptr) {
    return nullptr;
  }
  return reinterpret_cast<void (*)(void)>(dlsym(handle, zSymbol));
}

void fdbfs_sqlite3_xDlClose(sqlite3_vfs *, void *handle) {
  if (handle != nullptr) {
    dlclose(handle);
  }
}

int fdbfs_sqlite3_xRandomness(sqlite3_vfs *, int nByte, char *zOut) {
  if (zOut == nullptr || nByte <= 0) {
    return 0;
  }

  int total = 0;
  while (total < nByte) {
    const ssize_t rc =
        getrandom(zOut + total, static_cast<size_t>(nByte - total), 0);
    if (rc > 0) {
      total += static_cast<int>(rc);
      continue;
    }
    if (rc == -1 && errno == EINTR) {
      continue;
    }
    break;
  }

  if (total < nByte) {
    std::memset(zOut + total, 0, static_cast<size_t>(nByte - total));
  }
  return total;
}

int fdbfs_sqlite3_xSleep(sqlite3_vfs *, int microseconds) {
  if (microseconds > 0) {
    usleep(static_cast<useconds_t>(microseconds));
  }
  return microseconds;
}

int fdbfs_sqlite3_xCurrentTime(sqlite3_vfs *, double *time_out) {
  if (time_out == nullptr) {
    return SQLITE_IOERR;
  }
  *time_out = static_cast<double>(current_time_millis()) / 86400000.0;
  return SQLITE_OK;
}

int fdbfs_sqlite3_xGetLastError(sqlite3_vfs *, int nByte, char *zErrMsg) {
  if (zErrMsg != nullptr && nByte > 0) {
    zErrMsg[0] = '\0';
  }
  return 0;
}

int fdbfs_sqlite3_xCurrentTimeInt64(sqlite3_vfs *, sqlite3_int64 *time_out) {
  if (time_out == nullptr) {
    return SQLITE_IOERR;
  }
  *time_out = current_time_millis();
  return SQLITE_OK;
}

// IO Methods

int fdbfs_sqlite3_file_xClose(sqlite3_file *file) {
  if (file != nullptr) {
    file->pMethods = nullptr;
  }
  return SQLITE_OK;
}

int fdbfs_sqlite3_file_xRead(sqlite3_file *, void *, int, sqlite3_int64) {
  return SQLITE_IOERR_READ;
}

int fdbfs_sqlite3_file_xWrite(sqlite3_file *, const void *, int,
                              sqlite3_int64) {
  return SQLITE_IOERR_WRITE;
}

int fdbfs_sqlite3_file_xTruncate(sqlite3_file *, sqlite3_int64) {
  return SQLITE_IOERR_TRUNCATE;
}

int fdbfs_sqlite3_file_xSync(sqlite3_file *, int) { return SQLITE_IOERR_FSYNC; }

int fdbfs_sqlite3_file_xFileSize(sqlite3_file *, sqlite3_int64 *size_out) {
  if (size_out != nullptr) {
    *size_out = 0;
  }
  return SQLITE_IOERR_FSTAT;
}

int fdbfs_sqlite3_file_xLock(sqlite3_file *, int) { return SQLITE_OK; }

int fdbfs_sqlite3_file_xUnlock(sqlite3_file *, int) { return SQLITE_OK; }

int fdbfs_sqlite3_file_xCheckReservedLock(sqlite3_file *, int *result_out) {
  if (result_out != nullptr) {
    *result_out = 0;
  }
  return SQLITE_OK;
}

int fdbfs_sqlite3_file_xFileControl(sqlite3_file *, int, void *) {
  return SQLITE_NOTFOUND;
}

int fdbfs_sqlite3_file_xSectorSize(sqlite3_file *) { return 4096; }

int fdbfs_sqlite3_file_xDeviceCharacteristics(sqlite3_file *) { return 0; }

int fdbfs_sqlite3_file_xShmMap(sqlite3_file *, int, int, int,
                               void volatile **out) {
  if (out != nullptr) {
    *out = nullptr;
  }
  return SQLITE_IOERR_SHMMAP;
}

int fdbfs_sqlite3_file_xShmLock(sqlite3_file *, int, int, int) {
  return SQLITE_IOERR_SHMLOCK;
}

void fdbfs_sqlite3_file_xShmBarrier(sqlite3_file *) {}

int fdbfs_sqlite3_file_xShmUnmap(sqlite3_file *, int) {
  return SQLITE_IOERR_SHMOPEN;
}

int fdbfs_sqlite3_file_xFetch(sqlite3_file *, sqlite3_int64, int, void **out) {
  if (out != nullptr) {
    *out = nullptr;
  }
  return SQLITE_OK;
}

int fdbfs_sqlite3_file_xUnfetch(sqlite3_file *, sqlite3_int64, void *) {
  return SQLITE_OK;
}

const sqlite3_io_methods g_fdbfs_io_methods = {
    1,                                         // iVersion
    fdbfs_sqlite3_file_xClose,                 // xClose
    fdbfs_sqlite3_file_xRead,                  // xRead
    fdbfs_sqlite3_file_xWrite,                 // xWrite
    fdbfs_sqlite3_file_xTruncate,              // xTruncate
    fdbfs_sqlite3_file_xSync,                  // xSync
    fdbfs_sqlite3_file_xFileSize,              // xFileSize
    fdbfs_sqlite3_file_xLock,                  // xLock
    fdbfs_sqlite3_file_xUnlock,                // xUnlock
    fdbfs_sqlite3_file_xCheckReservedLock,     // xCheckReservedLock
    fdbfs_sqlite3_file_xFileControl,           // xFileControl
    fdbfs_sqlite3_file_xSectorSize,            // xSectorSize
    fdbfs_sqlite3_file_xDeviceCharacteristics, // xDeviceCharacteristics
    fdbfs_sqlite3_file_xShmMap,                // xShmMap
    fdbfs_sqlite3_file_xShmLock,               // xShmLock
    fdbfs_sqlite3_file_xShmBarrier,            // xShmBarrier
    fdbfs_sqlite3_file_xShmUnmap,              // xShmUnmap
    fdbfs_sqlite3_file_xFetch,                 // xFetch
    fdbfs_sqlite3_file_xUnfetch,               // xUnfetch
};
