#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <algorithm>
#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "fuse/directoryhandle.h"
#include "fuse/fdbfs_ops.h"
#include "fuse/filehandle.h"
#include "fuse/fuse_inflight_action.h"
#include "fuse/util_fuse.h"
#include "generic/forget.hpp"
#include "generic/fsync.h"
#include "generic/getinode.hpp"
#include "generic/getxattr.hpp"
#include "generic/link.hpp"
#include "generic/listxattr.hpp"
#include "generic/lookup.hpp"
#include "generic/mknod.hpp"
#include "generic/read.hpp"
#include "generic/readdir.hpp"
#include "generic/readdirplus.hpp"
#include "generic/readlink.hpp"
#include "generic/removexattr.hpp"
#include "generic/rename.hpp"
#include "generic/setattr.hpp"
#include "generic/setxattr.hpp"
#include "generic/statfs.hpp"
#include "generic/unlink.hpp"
#include "generic/util.h"
#include "generic/util_locks.h"
#include "generic/write.hpp"

namespace {
[[nodiscard]] inline fdbfs_ino_t to_fdbfs_ino(fuse_ino_t ino) {
  return static_cast<fdbfs_ino_t>(ino);
}
using INodeHandler = FuseInflightAction::INodeHandler;
using INodeHandlerAttr = FuseInflightAction::INodeHandlerAttr;
using INodeHandlerEntry = FuseInflightAction::INodeHandlerEntry;
using INodeHandlerOpen = FuseInflightAction::INodeHandlerOpen;
using INodeHandlerTmpfile = FuseInflightAction::INodeHandlerTmpfile;

LockManagerService *g_lock_manager_service = nullptr;

[[nodiscard]] inline SetAttrMask from_fuse_setattr_mask(int to_set) {
  uint32_t bits = 0;
  auto map_bit = [&](int fuse_flag, SetAttrBit bit) {
    if (to_set & fuse_flag) {
      bits |= static_cast<uint32_t>(bit);
    }
  };

  map_bit(FUSE_SET_ATTR_MODE, SetAttrBit::Mode);
  map_bit(FUSE_SET_ATTR_UID, SetAttrBit::Uid);
  map_bit(FUSE_SET_ATTR_GID, SetAttrBit::Gid);
  map_bit(FUSE_SET_ATTR_SIZE, SetAttrBit::Size);
  map_bit(FUSE_SET_ATTR_ATIME, SetAttrBit::Atime);
  map_bit(FUSE_SET_ATTR_MTIME, SetAttrBit::Mtime);
  map_bit(FUSE_SET_ATTR_ATIME_NOW, SetAttrBit::AtimeNow);
  map_bit(FUSE_SET_ATTR_MTIME_NOW, SetAttrBit::MtimeNow);
  map_bit(FUSE_SET_ATTR_FORCE, SetAttrBit::Force);
  map_bit(FUSE_SET_ATTR_CTIME, SetAttrBit::Ctime);
  map_bit(FUSE_SET_ATTR_KILL_SUID, SetAttrBit::KillSuid);
  map_bit(FUSE_SET_ATTR_KILL_SGID, SetAttrBit::KillSgid);
  map_bit(FUSE_SET_ATTR_FILE, SetAttrBit::File);
  map_bit(FUSE_SET_ATTR_KILL_PRIV, SetAttrBit::KillPriv);
  map_bit(FUSE_SET_ATTR_OPEN, SetAttrBit::Open);
  map_bit(FUSE_SET_ATTR_TIMES_SET, SetAttrBit::TimesSet);
  map_bit(FUSE_SET_ATTR_TOUCH, SetAttrBit::Touch);

  return SetAttrMask::from_raw(bits);
}
} // namespace

void fdbfs_set_lock_manager_service(LockManagerService *service) {
  g_lock_manager_service = service;
}

extern "C" void fdbfs_init(void *userdata, struct fuse_conn_info *conn) {
  (void)userdata;
  // transactions have to finish in under 5 seconds, so unless
  // we get clever and start splitting our reads across transactions
  // (which we're not currently set up for) then we need a limit
  // on the size of reads
  if (conn->max_read > 1024 * 1024)
    conn->max_read = 1024 * 1024;
  // per the docs, (write) transactions should be kept under 1MB.
  // let's stay well below that.
  if (conn->max_write > 128 * 1024)
    conn->max_write = 128 * 1024;
  conn->max_background = 256;
  conn->congestion_threshold = 192;
#if FUSE_VERSION >= 317
  if (conn->capable_ext & FUSE_CAP_ASYNC_DIO) {
    fuse_set_feature_flag(conn, FUSE_CAP_ASYNC_DIO);
  }
#endif
}

extern "C" void fdbfs_destroy(void *userdata) {
  (void)userdata;
  // no-op. main takes care of everything when the session loop ends.
}

// ==== lookup ====
extern "C" void fdbfs_lookup(fuse_req_t req, fuse_ino_t parent,
                             const char *name) {
  if (filename_length_check(name)) {
    fuse_reply_err(req, ENAMETOOLONG);
    return;
  }
  std::string sname(name);
  auto *inflight = new Inflight_lookup<FuseInflightAction, INodeHandler>(
      req, to_fdbfs_ino(parent), sname, make_transaction(),
      INodeHandlerEntry{});
  inflight->start();
}

// ==== getattr ====
extern "C" void fdbfs_getattr(fuse_req_t req, fuse_ino_t ino,
                              struct fuse_file_info *fi) {
  // get the file attributes of an inode
  auto *inflight = new Inflight_getinode<FuseInflightAction, INodeHandler>(
      req, to_fdbfs_ino(ino), make_transaction(), INodeHandlerAttr{});
  inflight->start();
}

extern "C" void fdbfs_statx(fuse_req_t req, fuse_ino_t ino, int flags, int mask,
                            struct fuse_file_info *fi) {
  (void)flags;
  (void)fi;
  auto *inflight = new Inflight_getinode<FuseInflightAction, INodeHandler>(
      req, to_fdbfs_ino(ino), make_transaction(),
      FuseInflightAction::INodeHandlerStatxMask{
          .mask = static_cast<uint32_t>(mask),
      });
  inflight->start();
}

// ==== readdir ====
template <template <typename> typename InflightT, bool DoPlus>
inline void fdbfs_readdir_common(fuse_req_t req, fuse_ino_t ino, size_t size,
                                 off_t off, struct fuse_file_info *fi) {
  auto dh = extract_fuse_handle<DirectoryHandle>(fi);
  if (!dh || dh->is_closed()) {
    fuse_reply_err(req, EBADF);
    return;
  }

  std::string_view start_name;
  ReaddirStartKind start_kind = ReaddirStartKind::Beginning;
  if (off < 0) {
    fuse_reply_err(req, EINVAL);
    return;
  }
  if (off < static_cast<off_t>(ReaddirStartKind::AfterName)) {
    start_kind = static_cast<ReaddirStartKind>(off);
  } else {
    auto start_name_opt = dh->cookie_to_filename(static_cast<uint64_t>(off));
    if (!start_name_opt.has_value()) {
      fuse_reply_err(req, EINVAL);
      return;
    }
    start_name = *start_name_opt;
    start_kind = ReaddirStartKind::AfterName;
  }

  const size_t collector_size =
      std::min(size, DoPlus ? static_cast<size_t>(1 << 12)
                            : static_cast<size_t>(1 << 16));
  auto collector_spec = FuseInflightAction::make_dirent_collector_spec(
      collector_size, DoPlus, dh);
  auto *inflight = new InflightT<FuseInflightAction>(
      req, to_fdbfs_ino(ino), collector_spec, start_kind, start_name,
      make_transaction());
  inflight->start();
}

extern "C" void fdbfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
                              off_t off, struct fuse_file_info *fi) {
  fdbfs_readdir_common<Inflight_readdir, false>(req, ino, size, off, fi);
}

// ==== readdirplus ====
extern "C" void fdbfs_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size,
                                  off_t off, struct fuse_file_info *fi) {
  fdbfs_readdir_common<Inflight_readdirplus, true>(req, ino, size, off, fi);
}

extern "C" void fdbfs_opendir(fuse_req_t req, fuse_ino_t ino,
                              struct fuse_file_info *fi) {
  auto handle = std::make_shared<DirectoryHandle>(ino);
  handle->reserve_cookies_through(
      static_cast<uint64_t>(ReaddirStartKind::AfterDotDot));
  auto slot = new std::shared_ptr<DirectoryHandle>(std::move(handle));
  *(extract_fuse_handle_slot<DirectoryHandle>(fi)) = slot;
  if (fuse_reply_open(req, fi) < 0) {
    free_fuse_handle_slot<DirectoryHandle>(fi);
  }
}

extern "C" void fdbfs_releasedir(fuse_req_t req, fuse_ino_t ino,
                                 struct fuse_file_info *fi) {
  (void)ino;
  auto dh = extract_fuse_handle<DirectoryHandle>(fi);
  if (!dh) {
    fuse_reply_err(req, EBADF);
    return;
  }
  dh->close();
  free_fuse_handle_slot<DirectoryHandle>(fi);
  fuse_reply_err(req, 0);
}

// ==== open ====
extern "C" void fdbfs_open(fuse_req_t req, fuse_ino_t ino,
                           struct fuse_file_info *fi) {
  if (fi->flags & O_TRUNC) {
    // truncate and time updates are handled through setattr logic.
    fdbfs_setattr_open_trunc(req, ino, fi);
    return;
  }

  // TODO any other flags we're passed that we could handle?
  if (fi->flags & O_APPEND) {
    // TODO need to test to see how this behaves with writes
  }
  (void)reply_open_with_handle(req, ino, fi);
}

// ==== read ====
extern "C" void fdbfs_read(fuse_req_t req, fuse_ino_t ino, size_t size,
                           off_t off, struct fuse_file_info *fi) {
  if (size == 0) {
    fuse_reply_buf(req, nullptr, 0);
    return;
  }
  if (off < 0) {
    fuse_reply_err(req, EINVAL);
    return;
  }

  auto fh = extract_fuse_handle<FileHandle>(fi);
  if (!fh) {
    fuse_reply_err(req, EBADF);
    return;
  }

  // given inode, figure out the appropriate key range, and
  // start reading it, filling it into a buffer to be sent back
  // with fuse_reply_buf
  auto *inflight = new Inflight_read<FuseInflightAction>(
      req, to_fdbfs_ino(ino), size, off, make_transaction());
  auto &filehandle = *fh;
  if (!filehandle.enqueue_inflight(inflight,
                                   offset_size_to_byte_range(off, size))) {
    delete inflight;
    fuse_reply_err(req, EBADF);
    return;
  }
}

// ==== mknod ====
extern "C" void fdbfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
                            mode_t mode, dev_t rdev) {
  if (filename_length_check(name)) {
    fuse_reply_err(req, ENAMETOOLONG);
    return;
  }
  filetype deduced_type;
  // validate mode value
  switch (mode & S_IFMT) {
  case S_IFSOCK:
    deduced_type = ft_socket;
    break;
  case S_IFLNK:
    deduced_type = ft_symlink;
    break;
  case S_IFREG:
    deduced_type = ft_regular;
    break;
  case S_IFCHR:
    deduced_type = ft_character;
    break;
  case S_IFIFO:
    deduced_type = ft_fifo;
    break;
  default: {
    // unsupported value. abort.
    fuse_reply_err(req, EPERM);
    return;
  }
  }

  auto *inflight = new Inflight_mknod<FuseInflightAction, INodeHandler>(
      req, to_fdbfs_ino(parent), std::string(name), mode & (~S_IFMT),
      deduced_type, rdev, make_transaction(), std::nullopt,
      INodeHandlerEntry{});
  inflight->start();
}

extern "C" void fdbfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                            mode_t mode) {
  if (filename_length_check(name)) {
    fuse_reply_err(req, ENAMETOOLONG);
    return;
  }
  auto *inflight = new Inflight_mknod<FuseInflightAction, INodeHandler>(
      req, to_fdbfs_ino(parent), std::string(name), mode & (~S_IFMT),
      ft_directory, 0, make_transaction(), std::nullopt, INodeHandlerEntry{});
  inflight->start();
}

extern "C" void fdbfs_tmpfile(fuse_req_t req, fuse_ino_t parent, mode_t mode,
                              struct fuse_file_info *fi) {
  auto *inflight = new Inflight_mknod<FuseInflightAction, INodeHandler>(
      req, to_fdbfs_ino(parent), std::nullopt, mode & (~S_IFMT), ft_regular, 0,
      make_transaction(), std::nullopt, INodeHandlerTmpfile{fi->flags});
  inflight->start();
}

extern "C" void fdbfs_symlink(fuse_req_t req, const char *target,
                              fuse_ino_t parent, const char *name) {
  // TODO eliminate magic number for symlink target length
  if (filename_length_check(target, 1024) || filename_length_check(name)) {
    fuse_reply_err(req, ENAMETOOLONG);
    return;
  }
  auto *inflight = new Inflight_mknod<FuseInflightAction, INodeHandler>(
      req, to_fdbfs_ino(parent), std::string(name), 0777 & (~S_IFMT),
      ft_symlink, 0, make_transaction(), std::string(target),
      INodeHandlerEntry{});
  inflight->start();
}

// ==== unlink ====
extern "C" void fdbfs_unlink(fuse_req_t req, fuse_ino_t ino, const char *name) {
  if (filename_length_check(name)) {
    fuse_reply_err(req, ENAMETOOLONG);
    return;
  }
  auto *inflight = new Inflight_unlink_rmdir<FuseInflightAction>(
      req, to_fdbfs_ino(ino), name, Op::Unlink, make_transaction());
  inflight->start();
}

extern "C" void fdbfs_rmdir(fuse_req_t req, fuse_ino_t ino, const char *name) {
  if (filename_length_check(name)) {
    fuse_reply_err(req, ENAMETOOLONG);
    return;
  }
  auto *inflight = new Inflight_unlink_rmdir<FuseInflightAction>(
      req, to_fdbfs_ino(ino), name, Op::Rmdir, make_transaction());
  inflight->start();
}

// ==== link ====
extern "C" void fdbfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                           const char *newname) {
  if (filename_length_check(newname)) {
    fuse_reply_err(req, ENAMETOOLONG);
    return;
  }
  auto *inflight = new Inflight_link<FuseInflightAction, INodeHandler>(
      req, to_fdbfs_ino(ino), to_fdbfs_ino(newparent), std::string(newname),
      make_transaction(), INodeHandlerEntry{});
  inflight->start();
}

// ==== readlink ====
extern "C" void fdbfs_readlink(fuse_req_t req, fuse_ino_t ino) {
  auto *inflight = new Inflight_readlink<FuseInflightAction>(
      req, to_fdbfs_ino(ino), make_transaction());
  inflight->start();
}

// ==== setattr ====
extern "C" void fdbfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                              int to_set, struct fuse_file_info *fi) {
  const SetAttrMask set_attr_mask = from_fuse_setattr_mask(to_set);
  auto *inflight = new Inflight_setattr<FuseInflightAction, INodeHandler>(
      req, to_fdbfs_ino(ino), *attr, set_attr_mask, make_transaction(),
      INodeHandlerAttr{});
  if (set_attr_mask.has(SetAttrBit::Size) && (fi != nullptr)) {
    if (attr->st_size < 0) {
      // admittedly unlikely we'll get a negative value from the kernel
      delete inflight;
      fuse_reply_err(req, EINVAL);
      return;
    }
    // we're doing a truncate on an open file, so we have to be serialized
    auto fh = extract_fuse_handle<FileHandle>(fi);
    if (!fh) {
      delete inflight;
      fuse_reply_err(req, EBADF);
      return;
    }
    auto &filehandle = *fh;
    if (!filehandle.enqueue_inflight(
            inflight, ByteRange::closed(attr->st_size,
                                        std::numeric_limits<off_t>::max()))) {
      delete inflight;
      fuse_reply_err(req, EBADF);
      return;
    }
  } else {
    // the no-serialization path, so we just fire it up
    inflight->start();
  }
}

extern "C" void fdbfs_setattr_open_trunc(fuse_req_t req, fuse_ino_t ino,
                                         struct fuse_file_info *fi) {
  struct stat attr{};
  attr.st_size = 0;
  // we're being called by open, so our 'fi' doesn't have a filehandle
  // in it, which is fine. we don't need to serialize this operation since
  // the file doesn't exist until we return. so there's nothing to serialize.
  auto *inflight = new Inflight_setattr<FuseInflightAction, INodeHandler>(
      req, to_fdbfs_ino(ino), attr, SetAttrMask(SetAttrBit::Size),
      make_transaction(), INodeHandlerOpen{fi->flags});
  inflight->start();
}

// ==== rename ====
extern "C" void fdbfs_rename(fuse_req_t req, fuse_ino_t parent,
                             const char *name, fuse_ino_t newparent,
                             const char *newname, unsigned int flags) {
  if (filename_length_check(name) || filename_length_check(newname)) {
    fuse_reply_err(req, ENAMETOOLONG);
    return;
  }
  auto *inflight = new Inflight_rename<FuseInflightAction>(
      req, to_fdbfs_ino(parent), std::string(name), to_fdbfs_ino(newparent),
      std::string(newname), flags, make_transaction());
  inflight->start();
}

// ==== write ====
extern "C" void fdbfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
                            size_t size, off_t off, struct fuse_file_info *fi) {
  if (size == 0) {
    // just in case?
    fuse_reply_write(req, 0);
    return;
  }
  if (off < 0) {
    fuse_reply_err(req, EINVAL);
    return;
  }

  auto fh = extract_fuse_handle<FileHandle>(fi);
  if (!fh) {
    fuse_reply_err(req, EBADF);
    return;
  }

  WritePos pos;
  if (fi->flags & O_APPEND) {
    pos = WritePosEOF{};
  } else {
    pos = WritePosOffset{.off = off};
  }

  std::vector<uint8_t> buffer(buf, buf + size);
  auto *inflight = new Inflight_write<FuseInflightAction>(
      req, to_fdbfs_ino(ino),
      WritePayloadBytes{.bytes = std::move(buffer)}, pos, make_transaction());

  auto &filehandle = *fh;
  if (!filehandle.enqueue_inflight(inflight,
                                   offset_size_to_byte_range(off, size))) {
    delete inflight;
    fuse_reply_err(req, EBADF);
    return;
  }
}

// ==== forget ====
extern "C" void fdbfs_forget(fuse_req_t req, fuse_ino_t ino, uint64_t ncount) {
  // we've only got to issue an fdb transaction if decrement says so
  auto generation = decrement_lookup_count(to_fdbfs_ino(ino), ncount);
  if (generation.has_value()) {
    std::vector<ForgetEntry> entries(1);
    entries[0] = ForgetEntry{to_fdbfs_ino(ino), *generation};
    auto *inflight = new Inflight_forget<FuseInflightAction>(
        req, std::move(entries), make_transaction());
    inflight->start();
  } else {
    fuse_reply_none(req);
  }
}

extern "C" void fdbfs_forget_multi(fuse_req_t req, size_t count,
                                   struct fuse_forget_data *forgets) {
  std::vector<ForgetEntry> entries;
  entries.reserve(count);
  for (size_t i = 0; i < count; i++) {
    auto generation = decrement_lookup_count(to_fdbfs_ino(forgets[i].ino),
                                             forgets[i].nlookup);
    if (generation.has_value()) {
      entries.push_back(ForgetEntry{to_fdbfs_ino(forgets[i].ino), *generation});
    }
  }
  if (entries.size() > 0) {
    // we've got to issue forgets
    auto *inflight = new Inflight_forget<FuseInflightAction>(
        req, std::move(entries), make_transaction());
    inflight->start();
  } else {
    fuse_reply_none(req);
  }
}

// ==== statfs ====
extern "C" void fdbfs_statfs(fuse_req_t req, fuse_ino_t ino) {
  auto *inflight =
      new Inflight_statfs<FuseInflightAction>(req, make_transaction());
  inflight->start();
}

// ==== getxattr ====
extern "C" void fdbfs_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                               size_t size) {
  if (filename_length_check(name)) {
    fuse_reply_err(req, ENAMETOOLONG);
    return;
  }

  auto *inflight = new Inflight_getxattr<FuseInflightAction>(
      req, to_fdbfs_ino(ino), name, size, make_transaction());
  inflight->start();
}

// ==== setxattr ====
extern "C" void fdbfs_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
                               const char *value, size_t size, int flags) {
  if (filename_length_check(name)) {
    fuse_reply_err(req, ENAMETOOLONG);
    return;
  }

  SetXattrBehavior behavior = CreateOrReplace;
  if (flags == XATTR_CREATE)
    behavior = CanCreate;
  else if (flags == XATTR_REPLACE)
    behavior = CanReplace;

  std::string sname(name);
  std::vector<uint8_t> vvalue(value, value + size);

  auto *inflight = new Inflight_setxattr<FuseInflightAction>(
      req, to_fdbfs_ino(ino), sname, vvalue, behavior, make_transaction());
  inflight->start();
}

// ==== removexattr ====
extern "C" void fdbfs_removexattr(fuse_req_t req, fuse_ino_t ino,
                                  const char *name) {
  if (filename_length_check(name)) {
    fuse_reply_err(req, ENAMETOOLONG);
    return;
  }

  std::string sname(name);
  auto *inflight = new Inflight_removexattr<FuseInflightAction>(
      req, to_fdbfs_ino(ino), sname, make_transaction());
  inflight->start();
}

// ==== listxattr ====
extern "C" void fdbfs_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
  if (size == 0) {
    auto *inflight = new Inflight_listxattr_count<FuseInflightAction>(
        req, to_fdbfs_ino(ino), make_transaction());
    inflight->start();
  } else {
    auto *inflight = new Inflight_listxattr<FuseInflightAction>(
        req, to_fdbfs_ino(ino), size, make_transaction());
    inflight->start();
  }
}

// ==== flush ====
extern "C" void fdbfs_flush(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi) {
  auto fh = extract_fuse_handle<FileHandle>(fi);
  if (!fh) {
    fuse_reply_err(req, EBADF);
    return;
  }

  // construct the appropriate callback for the barrier
  std::function<void(int)> barrier_callback;
  if (fi->lock_owner) {
    auto lock_owner = fi->lock_owner;
    barrier_callback = [req, ino, lock_owner](int err) {
      if (g_lock_manager_service == nullptr) {
        fuse_reply_err(req, EIO);
        return;
      }
      ByteRange range(0, std::numeric_limits<off_t>::max());
      g_lock_manager_service->queue_lock_manipulation(
          to_fdbfs_ino(ino), lock_owner, 0, 0, F_UNLCK, range,
          [req, err](std::expected<void, int> outcome) {
            fuse_reply_err(req, outcome ? err : outcome.error());
          });
    };
  } else {
    barrier_callback = [req](int err) { fuse_reply_err(req, err); };
  }

  if (!fh->enqueue_barrier(barrier_callback)) {
    fuse_reply_err(req, EBADF);
    return;
  }
}

// ==== fsync ====
extern "C" void fdbfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                            struct fuse_file_info *fi) {
  (void)datasync;
  (void)fi;
  g_fsync_barrier_table.fsync_async(
      to_fdbfs_ino(ino), [req](int err) { fuse_reply_err(req, err); });
}

extern "C" void fdbfs_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
                               struct fuse_file_info *fi) {
  (void)datasync;
  (void)fi;
  g_fsync_barrier_table.fsync_async(
      to_fdbfs_ino(ino), [req](int err) { fuse_reply_err(req, err); });
}

// ==== release ====
extern "C" void fdbfs_release(fuse_req_t req, fuse_ino_t ino,
                              struct fuse_file_info *fi) {
  auto fh = extract_fuse_handle<FileHandle>(fi);
  if (!fh) {
    fuse_reply_err(req, EBADF);
    return;
  }

  if (!fh->enqueue_barrier(
          [req, ino, fh](int err) {
            auto generation = decrement_lookup_count(to_fdbfs_ino(ino), 1);
            if (generation.has_value()) {
              best_effort_clear_inode_use_record(to_fdbfs_ino(ino),
                                                 *generation);
            }
            fh->close();
            fuse_reply_err(req, 0);
          },
          true)) {
    fuse_reply_err(req, EBADF);
    return;
  }

  free_fuse_handle_slot<FileHandle>(fi);
}

// ==== posix_locks ====
extern "C" void fdbfs_getlk(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi, struct flock *lock) {
  auto range = flock_to_range(lock);
  if (!range.has_value()) {
    fuse_reply_err(req, range.error());
    return;
  }

  auto conflict =
      (g_lock_manager_service == nullptr)
          ? std::nullopt
          : g_lock_manager_service->query_lock_conflict(
                to_fdbfs_ino(ino), fi->lock_owner, lock->l_type, range.value());
  if (!conflict.has_value()) {
    lock->l_type = F_UNLCK;
    lock->l_whence = SEEK_SET;
    lock->l_start = 0;
    lock->l_len = 0;
    lock->l_pid = 0;
    fuse_reply_lock(req, lock);
    return;
  }

  const off_t conflict_start = boost::icl::first(conflict->range);
  const off_t conflict_last = boost::icl::last(conflict->range);
  if ((conflict_start < 0) || (conflict_last < conflict_start)) {
    fuse_reply_err(req, EIO);
    return;
  }

  lock->l_type = conflict->locktype;
  lock->l_whence = SEEK_SET;
  lock->l_start = conflict_start;
  if (conflict_last == std::numeric_limits<off_t>::max()) {
    lock->l_len = 0;
  } else {
    lock->l_len = (conflict_last - conflict_start) + 1;
  }
  lock->l_pid = conflict->pid;
  fuse_reply_lock(req, lock);
}

extern "C" void fdbfs_setlk(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi, struct flock *lock,
                            int sleep) {
  if ((fi == NULL) || (lock == NULL)) {
    fuse_reply_err(req, EINVAL);
    return;
  }

  auto range = flock_to_range(lock);
  if (!range.has_value()) {
    fuse_reply_err(req, range.error());
    return;
  }

  if (g_lock_manager_service == nullptr) {
    fuse_reply_err(req, EIO);
    return;
  }

  g_lock_manager_service->queue_lock_manipulation(
      to_fdbfs_ino(ino), fi->lock_owner, lock->l_pid, sleep != 0, lock->l_type,
      range.value(), [req](std::expected<void, int> outcome) {
        fuse_reply_err(req, outcome ? 0 : outcome.error());
      });
}

#if 0
extern "C" void fdbfs_setlk(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi, struct flock *lock,
                            int sleep) {
  const auto lock_type_str = [](short t) -> const char * {
    switch (t) {
    case F_RDLCK:
      return "RDLCK";
    case F_WRLCK:
      return "WRLCK";
    case F_UNLCK:
      return "UNLCK";
    default:
      return "?";
    }
  };
  const auto whence_str = [](short w) -> const char * {
    switch (w) {
    case SEEK_SET:
      return "SET";
    case SEEK_CUR:
      return "CUR";
    case SEEK_END:
      return "END";
    default:
      return "?";
    }
  };

  if (lock != NULL) {
    fprintf(
        stdout,
        "fdbfs setlk ino=0x%" PRIx64 " owner=0x%" PRIx64
        " sleep=%d type=%s(%d) whence=%s(%d) start=%lld len=%lld pid=0x%lx\n",
        static_cast<uint64_t>(ino), (fi != NULL) ? fi->lock_owner : 0, sleep,
        lock_type_str(lock->l_type), static_cast<int>(lock->l_type),
        whence_str(lock->l_whence), static_cast<int>(lock->l_whence),
        static_cast<long long>(lock->l_start),
        static_cast<long long>(lock->l_len),
        static_cast<unsigned long>(lock->l_pid));
  } else {
    fprintf(stdout,
            "fdbfs setlk ino=0x%" PRIx64 " owner=0x%" PRIx64
            " sleep=%d lock=<null>\n",
            static_cast<uint64_t>(ino), (fi != NULL) ? fi->lock_owner : 0,
            sleep);
  }
  fflush(stdout);

  fuse_reply_err(req, 0);
}
#endif
