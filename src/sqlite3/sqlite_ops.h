#pragma once

#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <sqlite3ext.h>

#include "generic/fdbfs_runtime.h"
#include "generic/util.h"

struct fdbfs_file {
  sqlite3_file base; // must remain first

  fdbfs_ino_t ino;
  int currentlock = SQLITE_LOCK_NONE;

  fdbfs_file(fdbfs_ino_t ino) : ino(ino) {}
};

bool fdbfs_sqlite3_ensure_runtime();

int fdbfs_sqlite3_file_xClose(sqlite3_file *file);
int fdbfs_sqlite3_file_xRead(sqlite3_file *file, void *buf, int amount,
                             sqlite3_int64 offset);
int fdbfs_sqlite3_file_xWrite(sqlite3_file *file, const void *buf, int amount,
                              sqlite3_int64 offset);
int fdbfs_sqlite3_file_xTruncate(sqlite3_file *file, sqlite3_int64 size);
int fdbfs_sqlite3_file_xSync(sqlite3_file *file, int flags);
int fdbfs_sqlite3_file_xFileSize(sqlite3_file *file, sqlite3_int64 *size_out);
int fdbfs_sqlite3_file_xLock(sqlite3_file *file, int level);
int fdbfs_sqlite3_file_xUnlock(sqlite3_file *file, int level);
int fdbfs_sqlite3_file_xCheckReservedLock(sqlite3_file *file, int *result_out);
int fdbfs_sqlite3_file_xFileControl(sqlite3_file *file, int op, void *arg);
int fdbfs_sqlite3_file_xSectorSize(sqlite3_file *file);
int fdbfs_sqlite3_file_xDeviceCharacteristics(sqlite3_file *file);
int fdbfs_sqlite3_file_xShmMap(sqlite3_file *file, int page, int page_size,
                               int extend, void volatile **out);
int fdbfs_sqlite3_file_xShmLock(sqlite3_file *file, int offset, int n,
                                int flags);
void fdbfs_sqlite3_file_xShmBarrier(sqlite3_file *file);
int fdbfs_sqlite3_file_xShmUnmap(sqlite3_file *file, int delete_flag);
int fdbfs_sqlite3_file_xFetch(sqlite3_file *file, sqlite3_int64 offset,
                              int amount, void **out);
int fdbfs_sqlite3_file_xUnfetch(sqlite3_file *file, sqlite3_int64 offset,
                                void *ptr);

int fdbfs_sqlite3_xOpen(sqlite3_vfs *vfs, sqlite3_filename zName,
                        sqlite3_file *file, int flags, int *out_flags);
int fdbfs_sqlite3_xDelete(sqlite3_vfs *vfs, const char *zName, int sync_dir);
int fdbfs_sqlite3_xAccess(sqlite3_vfs *vfs, const char *zName, int flags,
                          int *result_out);
int fdbfs_sqlite3_xFullPathname(sqlite3_vfs *vfs, const char *zName, int nOut,
                                char *zOut);
void *fdbfs_sqlite3_xDlOpen(sqlite3_vfs *vfs, const char *zFilename);
void fdbfs_sqlite3_xDlError(sqlite3_vfs *vfs, int nByte, char *zErrMsg);
void (*fdbfs_sqlite3_xDlSym(sqlite3_vfs *vfs, void *handle,
                            const char *zSymbol))(void);
void fdbfs_sqlite3_xDlClose(sqlite3_vfs *vfs, void *handle);
int fdbfs_sqlite3_xRandomness(sqlite3_vfs *vfs, int nByte, char *zOut);
int fdbfs_sqlite3_xSleep(sqlite3_vfs *vfs, int microseconds);
int fdbfs_sqlite3_xCurrentTime(sqlite3_vfs *vfs, double *time_out);
int fdbfs_sqlite3_xGetLastError(sqlite3_vfs *vfs, int nByte, char *zErrMsg);
int fdbfs_sqlite3_xCurrentTimeInt64(sqlite3_vfs *vfs, sqlite3_int64 *time_out);

extern const sqlite3_io_methods g_fdbfs_io_methods;
