#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <cassert>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <dlfcn.h>
#include <errno.h>
#include <filesystem>
#include <optional>
#include <sys/random.h>
#include <unistd.h>

#include "generic/fsync.h"
#include "generic/getinode.hpp"
#include "generic/mknod.hpp"
#include "generic/read.hpp"
#include "generic/setattr.hpp"
#include "generic/unlink.hpp"
#include "generic/write.hpp"
#include "sqlite_inflight_action.h"
#include "sqlite_ops.h"
#include "util_sqlite.h"

// Special offsets in files used by sqlite for emulating
// its internal locking system using posix locks.
// it'd be nice if sqlite3 exposed these for VFS layers,
// especially since they're compiled into the core, but,
// no such luck.
#define PENDING_BYTE (0x40000000)
#define RESERVED_BYTE (PENDING_BYTE + 1)
#define SHARED_FIRST (PENDING_BYTE + 2)
#define SHARED_SIZE 510

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

std::future<SqliteResult> take_future(SqliteRequest *req) {
  return req->promise.get_future();
}

std::expected<SqliteReply, int>
wait_for_sqlite_result(std::future<SqliteResult> &future) {
  if (future.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
    return std::unexpected(ETIMEDOUT);
  }

  SqliteResult result = future.get();
  if (!result.has_value()) {
    return std::unexpected(result.error());
  }
  return *std::move(result);
}

std::optional<std::filesystem::path> normalize_sqlite_path(const char *zName) {
  if (zName == nullptr) {
    return std::nullopt;
  }

  std::filesystem::path path(zName);
  if (path.is_relative()) {
    path = std::filesystem::path("/") / path;
  }

  path = path.lexically_normal();
  return path;
}

} // namespace

// VFS Operations

int fdbfs_sqlite3_xOpen(sqlite3_vfs *, sqlite3_filename zName,
                        sqlite3_file *file, int flags, int *out_flags) {
  if (zName == nullptr || file == nullptr) {
    return SQLITE_CANTOPEN;
  }

  std::memset(file, 0, sizeof(struct fdbfs_file));
  if (out_flags != nullptr) {
    *out_flags = 0;
  }

  auto normalized = normalize_sqlite_path(zName);
  if (!normalized.has_value() || !normalized->has_filename()) {
    return SQLITE_CANTOPEN;
  }

  const std::filesystem::path parent_path = normalized->parent_path();
  const std::string filename = normalized->filename().string();
  if (filename.empty() || filename == "." || filename == "..") {
    return SQLITE_CANTOPEN;
  }

  auto parent = resolve_path(parent_path, true);
  if (!parent.has_value()) {
    return SQLITE_CANTOPEN;
  }

  auto forget_parent = [&parent]() {
    forget_inodes_best_effort({parent->inode()});
  };

  auto target = lookup_inode(parent->inode(), filename);
  if (!target.has_value()) {
    if ((flags & SQLITE_OPEN_CREATE) == 0) {
      forget_parent();
      return SQLITE_CANTOPEN;
    }
    if ((parent->mode() & 0300) != 0300) {
      forget_parent();
      return SQLITE_PERM;
    }

    auto req = std::make_unique<SqliteRequest>();
    auto future = take_future(req.get());
    auto *inflight =
        new Inflight_mknod<SqliteInflightAction,
                           SqliteInflightAction::INodeHandlerEntry>(
            req.get(), parent->inode(), filename, 0644, ft_regular, 0,
            make_transaction(), std::nullopt,
            SqliteInflightAction::INodeHandlerEntry{});
    inflight->start();

    auto result = wait_for_sqlite_result(future);
    forget_parent();
    if (!result.has_value()) {
      return SQLITE_CANTOPEN;
    }
    if (!std::holds_alternative<SqliteReplyINode>(*result)) {
      return SQLITE_CANTOPEN;
    }
    target = std::get<SqliteReplyINode>(*result).inode;
  } else {
    forget_parent();
  }

  if (target->type() != ft_regular) {
    return SQLITE_CANTOPEN;
  }

  if ((flags & SQLITE_OPEN_READWRITE) != 0) {
    if ((target->mode() & 0600) != 0600) {
      return SQLITE_PERM;
    }
  } else if ((flags & SQLITE_OPEN_READONLY) != 0) {
    if ((target->mode() & 0400) != 0400) {
      return SQLITE_PERM;
    }
  }

  struct fdbfs_file *p = new (file) fdbfs_file(target->inode());
  p->base.pMethods = &g_fdbfs_io_methods;
  if (out_flags != nullptr) {
    *out_flags = flags;
  }
  return SQLITE_OK;
}

int fdbfs_sqlite3_xDelete(sqlite3_vfs *, const char *zName, int syncdir) {
  (void)syncdir;

  if (zName == nullptr) {
    return SQLITE_IOERR_DELETE;
  }

  auto normalized = normalize_sqlite_path(zName);
  if (!normalized.has_value() || !normalized->has_filename()) {
    return SQLITE_IOERR_DELETE;
  }

  const std::filesystem::path parent_path = normalized->parent_path();
  const std::string filename = normalized->filename().string();
  if (filename.empty() || filename == "." || filename == "..") {
    return SQLITE_IOERR_DELETE;
  }

  auto parent = resolve_path(parent_path, true);
  if (!parent.has_value()) {
    return SQLITE_IOERR_DELETE;
  }

  if (!(parent->mode() & 0300)) {
    // we act on owner permissions; so if the owner of the
    // parent directory doesn't have permission to delete the
    // file, then we refuse.
    return SQLITE_IOERR_DELETE;
  }

  auto req = std::make_unique<SqliteRequest>();
  auto future = take_future(req.get());
  auto *inflight = new Inflight_unlink_rmdir<SqliteInflightAction>(
      req.get(), parent->inode(), filename, Op::Unlink, make_transaction());
  inflight->start();

  auto result = wait_for_sqlite_result(future);
  forget_inodes_best_effort({parent->inode()});
  if (!result.has_value()) {
    return SQLITE_IOERR_DELETE;
  }
  return SQLITE_OK;
}

int fdbfs_sqlite3_xAccess(sqlite3_vfs *, const char *zName, int flags,
                          int *result_out) {
  if (zName == nullptr || result_out == nullptr) {
    return SQLITE_IOERR_ACCESS;
  }

  *result_out = 0;

  auto normalized = normalize_sqlite_path(zName);
  if (!normalized.has_value()) {
    return SQLITE_OK;
  }

  auto inode = resolve_path(*normalized);
  if (!inode.has_value()) {
    return SQLITE_OK;
  }

  switch (flags) {
  case SQLITE_ACCESS_EXISTS:
    *result_out = 1;
    break;
  case SQLITE_ACCESS_READ:
    *result_out = ((inode->mode() & S_IRUSR) != 0);
    break;
  case SQLITE_ACCESS_READWRITE:
    *result_out =
        (((inode->mode() & S_IRUSR) != 0) && ((inode->mode() & S_IWUSR) != 0));
    break;
  default:
    *result_out = 0;
    break;
  }

  return SQLITE_OK;
}

int fdbfs_sqlite3_xFullPathname(sqlite3_vfs *, const char *zName, int nOut,
                                char *zOut) {
  if (zName == nullptr || zOut == nullptr || nOut <= 0) {
    return SQLITE_CANTOPEN;
  }

  auto normalized = normalize_sqlite_path(zName);
  if (!normalized.has_value() || !normalized->has_filename()) {
    return SQLITE_CANTOPEN;
  }

  const std::string normalized_name = normalized->string();
  if (normalized_name.size() + 1 > static_cast<size_t>(nOut)) {
    return SQLITE_ERROR;
  }

  std::memcpy(zOut, normalized_name.c_str(), normalized_name.size() + 1);
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

int fdbfs_sqlite3_file_xClose(sqlite3_file *file_) {
  if (file_ == nullptr) {
    return SQLITE_MISUSE;
  }
  struct fdbfs_file *file = reinterpret_cast<struct fdbfs_file *>(file_);
  forget_inodes_best_effort({file->ino});
  file->~fdbfs_file();
  return SQLITE_OK;
}

int fdbfs_sqlite3_file_xRead(sqlite3_file *file_, void *buf, int iAmt,
                             sqlite3_int64 iOfst) {
  if (file_ == nullptr || buf == nullptr) {
    return SQLITE_MISUSE;
  }
  assert(iAmt <= 1024 * 1024);
  struct fdbfs_file *file = reinterpret_cast<struct fdbfs_file *>(file_);
  if (iAmt < 0 || iOfst < 0) {
    return SQLITE_IOERR_READ;
  }

  auto req = std::make_unique<SqliteRequest>();
  auto future = take_future(req.get());
  auto *inflight = new Inflight_read<SqliteInflightAction>(
      req.get(), file->ino, static_cast<size_t>(iAmt),
      static_cast<off_t>(iOfst), make_transaction());
  inflight->start();

  auto result = wait_for_sqlite_result(future);
  if (!result.has_value()) {
    return SQLITE_IOERR_READ;
  }
  if (!std::holds_alternative<SqliteReplyBuf>(*result)) {
    return SQLITE_IOERR_READ;
  }

  auto &reply = std::get<SqliteReplyBuf>(*result);
  const size_t got = std::min(reply.bytes.size(), static_cast<size_t>(iAmt));
  if (got > 0) {
    std::memcpy(buf, reply.bytes.data(), got);
  }
  if (got < static_cast<size_t>(iAmt)) {
    std::memset(static_cast<uint8_t *>(buf) + got, 0,
                static_cast<size_t>(iAmt) - got);
    return SQLITE_IOERR_SHORT_READ;
  }
  return SQLITE_OK;
}

int fdbfs_sqlite3_file_xWrite(sqlite3_file *file_, const void *buf, int iAmt,
                              sqlite3_int64 iOfst) {
  if (file_ == nullptr || buf == nullptr) {
    return SQLITE_MISUSE;
  }
  assert(iAmt <= 128 * 1024);
  struct fdbfs_file *file = reinterpret_cast<struct fdbfs_file *>(file_);
  if (iAmt < 0 || iOfst < 0) {
    return SQLITE_IOERR_WRITE;
  }

  const uint8_t *start = static_cast<const uint8_t *>(buf);
  std::vector<uint8_t> bytes(start, start + iAmt);

  auto req = std::make_unique<SqliteRequest>();
  auto future = take_future(req.get());
  auto *inflight = new Inflight_write<SqliteInflightAction>(
      req.get(), file->ino, std::move(bytes),
      WritePosOffset{.off = static_cast<off_t>(iOfst)}, make_transaction());
  inflight->start();

  auto result = wait_for_sqlite_result(future);
  if (!result.has_value()) {
    return SQLITE_IOERR_WRITE;
  }
  if (!std::holds_alternative<SqliteReplyWrite>(*result)) {
    return SQLITE_IOERR_WRITE;
  }
  if (std::get<SqliteReplyWrite>(*result).size != static_cast<size_t>(iAmt)) {
    return SQLITE_IOERR_WRITE;
  }
  return SQLITE_OK;
}

int fdbfs_sqlite3_file_xTruncate(sqlite3_file *file_, sqlite3_int64 size) {
  if (file_ == nullptr) {
    return SQLITE_MISUSE;
  }
  struct fdbfs_file *file = reinterpret_cast<struct fdbfs_file *>(file_);

  struct stat attr{};
  attr.st_size = size;

  auto req = std::make_unique<SqliteRequest>();
  auto future = take_future(req.get());
  auto *inflight = new Inflight_setattr<SqliteInflightAction, std::monostate>(
      req.get(), file->ino, attr, SetAttrMask(SetAttrBit::Size),
      make_transaction(), std::monostate{});
  inflight->start();

  auto result = wait_for_sqlite_result(future);
  if (!result.has_value()) {
    return SQLITE_IOERR_TRUNCATE;
  }
  return SQLITE_OK;
}

int fdbfs_sqlite3_file_xSync(sqlite3_file *file_, int flags) {
  if (file_ == nullptr) {
    return SQLITE_MISUSE;
  }
  struct fdbfs_file *file = reinterpret_cast<struct fdbfs_file *>(file_);
  fdbfs_ino_t ino = file->ino;
  (void)flags;

  auto promise = std::make_shared<std::promise<int>>();
  auto future = promise->get_future();

  g_fsync_barrier_table.fsync_async(ino, [promise](int err) {
    try {
      promise->set_value(err);
    } catch (const std::future_error &) {
      std::terminate();
    }
  });

  if (future.get()) {
    return SQLITE_IOERR_FSYNC;
  }
  return SQLITE_OK;
}

int fdbfs_sqlite3_file_xFileSize(sqlite3_file *file_, sqlite3_int64 *size_out) {
  if (file_ == nullptr || size_out == nullptr) {
    return SQLITE_MISUSE;
  }
  struct fdbfs_file *file = reinterpret_cast<struct fdbfs_file *>(file_);

  auto req = std::make_unique<SqliteRequest>();
  auto future = take_future(req.get());
  auto *inflight = new Inflight_getinode<SqliteInflightAction, std::monostate>(
      req.get(), file->ino, make_transaction(), std::monostate{});
  inflight->start();

  auto result = wait_for_sqlite_result(future);
  if (!result.has_value()) {
    *size_out = 0;
    return SQLITE_IOERR_FSTAT;
  }
  if (!std::holds_alternative<SqliteReplyINode>(*result)) {
    *size_out = 0;
    return SQLITE_IOERR_FSTAT;
  }

  *size_out = static_cast<sqlite3_int64>(
      std::get<SqliteReplyINode>(*result).inode.size());
  return SQLITE_OK;
}

int fdbfs_sqlite3_file_xLock(sqlite3_file *file_, int mode) {
  if (file_ == nullptr) {
    return SQLITE_MISUSE;
  }
  struct fdbfs_file *file = reinterpret_cast<struct fdbfs_file *>(file_);
  (void)file;
  (void)mode;
  return SQLITE_OK;
}

int fdbfs_sqlite3_file_xUnlock(sqlite3_file *file_, int mode) {
  if (file_ == nullptr) {
    return SQLITE_MISUSE;
  }
  struct fdbfs_file *file = reinterpret_cast<struct fdbfs_file *>(file_);
  (void)file;
  (void)mode;
  return SQLITE_OK;
}

int fdbfs_sqlite3_file_xCheckReservedLock(sqlite3_file *file_,
                                          int *result_out) {
  if (file_ == nullptr || result_out == nullptr) {
    return SQLITE_MISUSE;
  }
  struct fdbfs_file *file = reinterpret_cast<struct fdbfs_file *>(file_);
  (void)file;
  if (result_out != nullptr) {
    *result_out = 0;
  }
  return SQLITE_OK;
}

int fdbfs_sqlite3_file_xFileControl(sqlite3_file *file_, int, void *) {
  if (file_ == nullptr) {
    return SQLITE_MISUSE;
  }
  struct fdbfs_file *file = reinterpret_cast<struct fdbfs_file *>(file_);
  (void)file;
  // we could someday do SQLITE_FCNTL_{BEGIN,COMMIT}_ATOMIC_WRITE, but
  // we'll need to check and see how many writes sqlite might want to
  // cram into a single batch.
  return SQLITE_NOTFOUND;
}

int fdbfs_sqlite3_file_xSectorSize(sqlite3_file *) { return BLOCKSIZE; }

int fdbfs_sqlite3_file_xDeviceCharacteristics(sqlite3_file *) {
  // NOTE
  // this is the same for all files, as they rely on properties we guarantee
  // for all files.
  // we don't have SQLITE_IOCAP_BATCH_ATOMIC at the moment, but we could
  // possibly implement that later.
  constexpr int characteristics =
      SQLITE_IOCAP_ATOMIC512 | SQLITE_IOCAP_ATOMIC1K | SQLITE_IOCAP_ATOMIC2K |
      SQLITE_IOCAP_ATOMIC4K | SQLITE_IOCAP_ATOMIC8K | SQLITE_IOCAP_ATOMIC16K |
      SQLITE_IOCAP_ATOMIC32K | SQLITE_IOCAP_ATOMIC64K |
      SQLITE_IOCAP_SAFE_APPEND | SQLITE_IOCAP_SEQUENTIAL |
      SQLITE_IOCAP_POWERSAFE_OVERWRITE
#ifdef SQLITE_IOCAP_SUBPAGE_READ
      // This is always fine, so if we're compiled against a sqlite that has it,
      // then great, we offer it.
      | SQLITE_IOCAP_SUBPAGE_READ
#endif
      ;

  return characteristics;
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
    // we're only implementing v1 IO methods. we don't have,
    // and literally cannot support, any of the SHM or fetch
    // operations.
    nullptr, // xShmMap
    nullptr, // xShmLock
    nullptr, // xShmBarrier
    nullptr, // xShmUnmap
    nullptr, // xFetch
    nullptr, // xUnfetch
};
