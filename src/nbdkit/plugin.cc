#define FDB_API_VERSION 730
#define NBDKIT_API_VERSION 2
#define THREAD_MODEL NBDKIT_THREAD_MODEL_PARALLEL

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <nbdkit-plugin.h>

#include "nbdkit/nbdkit_inflight_action.h"

#include "generic/fdb_service.h"
#include "generic/fdbfs_runtime.h"
#include "generic/forget.hpp"
#include "generic/fsync.h"
#include "generic/garbage_collector.h"
#include "generic/getinode.hpp"
#include "generic/liveness.h"
#include "generic/lookup.hpp"
#include "generic/read.hpp"
#include "generic/readdirplus.hpp"
#include "generic/util.h"
#include "generic/util_locks.h"
#include "generic/void_inflight_action.h"
#include "generic/write.hpp"

uint8_t BLOCKBITS = 13;
uint32_t BLOCKSIZE = 1u << 13;

namespace {

constexpr auto kInflightTimeout = std::chrono::seconds(5);
constexpr fdbfs_ino_t kRootIno = 1;
constexpr std::string_view kDefaultExport = "";
constexpr std::string_view kDefaultKeyPrefix = "FS";
constexpr size_t kListExportsBatchSize = 128;

struct PluginConfig {
  std::optional<std::string> key_prefix;

  int set_option(const char *key, const char *value) {
    if ((key == nullptr) || (value == nullptr)) {
      nbdkit_set_error(EINVAL);
      return -1;
    }

    std::string_view key_sv(key);
    if (key_sv == "key_prefix") {
      if (value[0] == '\0') {
        nbdkit_set_error(EINVAL);
        return -1;
      }
      key_prefix = value;
      return 0;
    }

    nbdkit_set_error(EINVAL);
    return -1;
  }

  int finalize() {
    if (!key_prefix.has_value()) {
      key_prefix = std::string(kDefaultKeyPrefix);
    }
    if (key_prefix->empty()) {
      nbdkit_set_error(EINVAL);
      return -1;
    }
    return 0;
  }

  std::string_view selected_key_prefix() const {
    if (key_prefix.has_value()) {
      return *key_prefix;
    }
    return kDefaultKeyPrefix;
  }
};

struct Handle {
  explicit Handle(fdbfs_ino_t ino, struct stat attr) : ino(ino), attr(attr) {}

  fdbfs_ino_t ino;
  struct stat attr;

  ~Handle() {
    try {
      auto generation = decrement_lookup_count(ino, 1);
      if (!generation.has_value()) {
        return;
      }

      std::vector<ForgetEntry> entries;
      entries.push_back(ForgetEntry{.ino = ino, .generation = *generation});
      auto *inflight = new Inflight_forget<VoidInflightAction>(
          std::monostate{}, std::move(entries), make_transaction());
      inflight->start();
    } catch (...) {
      // Best-effort cleanup only.
    }
  }
};

PluginConfig g_config;
std::unique_ptr<FdbfsRuntime> g_runtime_owner;

std::future<NbdkitResult> take_future(NbdkitRequest *req) {
  return req->promise.get_future();
}

std::expected<NbdkitReply, int>
wait_for_nbdkit_result(std::future<NbdkitResult> &future) {
  if (future.wait_for(kInflightTimeout) != std::future_status::ready) {
    return std::unexpected(ETIMEDOUT);
  }

  NbdkitResult result = future.get();
  if (!result.has_value()) {
    return std::unexpected(result.error());
  }
  return *std::move(result);
}

void initialize_generic_state() {
  const std::string_view selected_key_prefix = g_config.selected_key_prefix();

  key_prefix.clear();
  key_prefix.insert(key_prefix.end(), selected_key_prefix.begin(),
                    selected_key_prefix.end());

  inode_key_length = pack_inode_key(0).size();
  fileblock_prefix_length = inode_key_length;
  fileblock_key_length = pack_fileblock_key(0, 0).size();
  dirent_prefix_length = pack_dentry_key(0, "").size();

  BLOCKBITS = 13;
  BLOCKSIZE = 1u << BLOCKBITS;

  lookup_counts.reserve(128);
}

static void fdbfs_load(void) { GOOGLE_PROTOBUF_VERIFY_VERSION; }

static void fdbfs_unload(void) {}

static int fdbfs_config(const char *key, const char *value) {
  return g_config.set_option(key, value);
}

static int fdbfs_config_complete(void) { return g_config.finalize(); }

static void *fdbfs_open(int readonly) {
  (void)readonly;

  const char *export_name = nbdkit_export_name();
  if ((export_name == nullptr) || (export_name[0] == '\0')) {
    nbdkit_set_error(ENOENT);
    return nullptr;
  }

  auto lookup_req = std::make_unique<NbdkitRequest>();
  auto lookup_future = take_future(lookup_req.get());
  auto *lookup = new Inflight_lookup<NbdkitInflightAction, std::monostate>(
      lookup_req.get(), kRootIno, std::string(export_name), make_transaction(),
      std::monostate{});
  lookup->start();

  auto lookup_reply = wait_for_nbdkit_result(lookup_future);
  if (!lookup_reply.has_value()) {
    nbdkit_set_error(lookup_reply.error());
    return nullptr;
  }
  if (!std::holds_alternative<NbdkitReplyINode>(*lookup_reply)) {
    nbdkit_set_error(EIO);
    return nullptr;
  }

  const auto target_ino =
      std::get<NbdkitReplyINode>(*lookup_reply).inode.inode();

  auto inode_req = std::make_unique<NbdkitRequest>();
  auto inode_future = take_future(inode_req.get());
  auto *getinode =
      new Inflight_getinode<NbdkitInflightAction,
                            NbdkitInflightAction::INodeHandlerOpen>(
          inode_req.get(), target_ino, make_transaction(),
          NbdkitInflightAction::INodeHandlerOpen{});
  getinode->start();

  auto inode_reply = wait_for_nbdkit_result(inode_future);
  if (!inode_reply.has_value()) {
    nbdkit_set_error(inode_reply.error());
    return nullptr;
  }
  if (!std::holds_alternative<NbdkitReplyINode>(*inode_reply)) {
    nbdkit_set_error(EIO);
    return nullptr;
  }

  auto &opened = std::get<NbdkitReplyINode>(*inode_reply);
  if ((opened.attr.st_mode & S_IFMT) != S_IFREG) {
    nbdkit_set_error(EISDIR);
    return nullptr;
  }

  auto handle = std::make_unique<Handle>(opened.inode.inode(), opened.attr);
  return handle.release();
}

static void fdbfs_close(void *handle) { delete static_cast<Handle *>(handle); }

static int64_t fdbfs_get_size(void *handle) {
  const auto *h = static_cast<const Handle *>(handle);
  return h->attr.st_size;
}

static int fdbfs_can_write(void *handle) {
  (void)handle;
  return 1;
}

static int fdbfs_can_flush(void *handle) {
  (void)handle;
  return 1;
}

static int fdbfs_is_rotational(void *handle) {
  (void)handle;
  return 0;
}

static int fdbfs_can_trim(void *handle) {
  (void)handle;
  return 1;
}

static int fdbfs_pread(void *handle, void *buf, uint32_t count, uint64_t offset,
                       uint32_t flags) {
  (void)flags;

  if ((handle == nullptr) || (buf == nullptr)) {
    nbdkit_set_error(EINVAL);
    return -1;
  }

  auto *h = static_cast<Handle *>(handle);
  const uint64_t file_size = static_cast<uint64_t>(h->attr.st_size);
  if ((offset > file_size) || ((file_size - offset) < count)) {
    nbdkit_set_error(EIO);
    return -1;
  }
  if (count == 0) {
    return 0;
  }
  if (offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    nbdkit_set_error(EIO);
    return -1;
  }

  auto req = std::make_unique<NbdkitRequest>();
  auto future = take_future(req.get());
  auto *inflight = new Inflight_read<NbdkitInflightAction>(
      req.get(), h->ino, count, static_cast<off_t>(offset), make_transaction());
  inflight->start();

  auto reply = wait_for_nbdkit_result(future);
  if (!reply.has_value()) {
    if (req->fdb_error.has_value()) {
      nbdkit_error("FDB error: %s", fdb_get_error(*req->fdb_error));
      nbdkit_set_error(EIO);
    } else {
      nbdkit_set_error(reply.error());
    }
    return -1;
  }
  if (!std::holds_alternative<NbdkitReplyBuf>(*reply)) {
    nbdkit_set_error(EIO);
    return -1;
  }

  auto &read_reply = std::get<NbdkitReplyBuf>(*reply);
  if (read_reply.bytes.size() != count) {
    nbdkit_set_error(EIO);
    return -1;
  }

  // TODO modify Inflight_read so that it can have a buffer
  // passed into it, which it fills directly, saving us this
  // copy.
  std::memcpy(buf, read_reply.bytes.data(), count);
  return 0;
}

static int fdbfs_pwrite(void *handle, const void *buf, uint32_t count,
                        uint64_t offset, uint32_t flags) {
  (void)flags;

  if ((handle == nullptr) || (buf == nullptr)) {
    nbdkit_set_error(EINVAL);
    return -1;
  }

  auto *h = static_cast<Handle *>(handle);
  const uint64_t file_size = static_cast<uint64_t>(h->attr.st_size);
  if ((offset > file_size) || ((file_size - offset) < count)) {
    nbdkit_set_error(EIO);
    return -1;
  }
  if (count == 0) {
    return 0;
  }
  if (offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    nbdkit_set_error(EIO);
    return -1;
  }

  const auto *bytes = static_cast<const uint8_t *>(buf);
  std::vector<uint8_t> write_buf(bytes, bytes + count);

  auto req = std::make_unique<NbdkitRequest>();
  auto future = take_future(req.get());
  auto *inflight = new Inflight_write<NbdkitInflightAction>(
      req.get(), h->ino, WritePayloadBytes{.bytes = std::move(write_buf)},
      WritePosOffset{.off = static_cast<off_t>(offset)}, make_transaction());
  inflight->start();

  auto reply = wait_for_nbdkit_result(future);
  if (!reply.has_value()) {
    if (req->fdb_error.has_value()) {
      nbdkit_error("FDB error: %s", fdb_get_error(*req->fdb_error));
      nbdkit_set_error(EIO);
    } else {
      nbdkit_set_error(reply.error());
    }
    return -1;
  }
  if (!std::holds_alternative<NbdkitReplyWrite>(*reply)) {
    nbdkit_set_error(EIO);
    return -1;
  }
  if (std::get<NbdkitReplyWrite>(*reply).size != count) {
    nbdkit_set_error(EIO);
    return -1;
  }
  return 0;
}

static int fdbfs_flush(void *handle, uint32_t flags) {
  (void)flags;

  if (handle == nullptr) {
    nbdkit_set_error(EINVAL);
    return -1;
  }

  auto *h = static_cast<Handle *>(handle);
  auto promise = std::make_shared<std::promise<int>>();
  auto future = promise->get_future();

  g_fsync_barrier_table.fsync_async(h->ino, [promise](int err) {
    try {
      promise->set_value(err);
    } catch (const std::future_error &) {
      std::terminate();
    }
  });

  if (future.get() != 0) {
    nbdkit_set_error(EIO);
    return -1;
  }
  return 0;
}

static int fdbfs_zero(void *handle, uint32_t count, uint64_t offset,
                      uint32_t flags) {
  (void)flags;

  if (handle == nullptr) {
    nbdkit_set_error(EINVAL);
    return -1;
  }

  auto *h = static_cast<Handle *>(handle);
  const uint64_t file_size = static_cast<uint64_t>(h->attr.st_size);

  if (count == 0) {
    return 0;
  }
  if (offset > file_size) {
    nbdkit_set_error(EINVAL);
    return -1;
  }
  if (count > (file_size - offset)) {
    nbdkit_set_error(EINVAL);
    return -1;
  }
  if (offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) {
    nbdkit_set_error(EIO);
    return -1;
  }

  auto req = std::make_unique<NbdkitRequest>();
  auto future = take_future(req.get());
  auto *inflight = new Inflight_write<NbdkitInflightAction>(
      req.get(), h->ino, WritePayloadZeroes{.size = count},
      WritePosOffset{.off = static_cast<off_t>(offset)}, make_transaction());
  inflight->start();

  auto reply = wait_for_nbdkit_result(future);
  if (!reply.has_value()) {
    if (req->fdb_error.has_value()) {
      nbdkit_error("FDB error: %s", fdb_get_error(*req->fdb_error));
      nbdkit_set_error(EIO);
    } else {
      nbdkit_set_error(reply.error());
    }
    return -1;
  }
  if (!std::holds_alternative<NbdkitReplyWrite>(*reply)) {
    nbdkit_set_error(EIO);
    return -1;
  }
  if (std::get<NbdkitReplyWrite>(*reply).size != count) {
    nbdkit_set_error(EIO);
    return -1;
  }
  return 0;
}

static int fdbfs_trim(void *handle, uint32_t count, uint64_t offset,
                      uint32_t flags) {
  return fdbfs_zero(handle, count, offset, flags);
}

static void fdbfs_dump_plugin(void) {}

static int fdbfs_can_zero(void *handle) {
  (void)handle;
  return 1;
}

static int fdbfs_can_fua(void *handle) {
  (void)handle;
  // we always wait to return until the data
  // is persisted.
  return NBDKIT_FUA_NATIVE;
}

static int fdbfs_can_multi_conn(void *handle) {
  (void)handle;
  return 1;
}

static int fdbfs_can_cache(void *handle) {
  (void)handle;
  // we don't maintain any sort of cache, so
  // even asking nbdkit to emulate the call with
  // pread is unproductive.
  return NBDKIT_CACHE_NONE;
}

static int fdbfs_can_fast_zero(void *handle) {
  (void)handle;
  return 1;
}

static int fdbfs_preconnect(int readonly) {
  (void)readonly;
  return 0;
}

static int fdbfs_get_ready(void) {
  initialize_generic_state();
  return 0;
}

static int fdbfs_after_fork(void) {
  if (g_runtime_owner) {
    return 0;
  }

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
    g_fdbfs_runtime = runtime.get();
    runtime->start_all();
    g_runtime_owner = std::move(runtime);
    return 0;
  } catch (...) {
    nbdkit_set_error(EIO);
    return -1;
  }
}

static int fdbfs_list_exports(int readonly, int is_tls,
                              struct nbdkit_exports *exports) {
  (void)readonly;
  (void)is_tls;

  ReaddirStartKind start_kind = ReaddirStartKind::AfterDotDot;
  std::string start_name;

  for (;;) {
    auto req = std::make_unique<NbdkitRequest>();
    auto future = take_future(req.get());
    auto *inflight = new Inflight_readdirplus<NbdkitInflightAction>(
        req.get(), kRootIno,
        NbdkitInflightAction::make_dirent_collector_spec(kListExportsBatchSize,
                                                         true),
        start_kind, start_name, make_transaction());
    inflight->start();

    auto reply = wait_for_nbdkit_result(future);
    if (!reply.has_value()) {
      if (req->fdb_error.has_value()) {
        nbdkit_error("FDB error: %s", fdb_get_error(*req->fdb_error));
        nbdkit_set_error(EIO);
      } else {
        nbdkit_set_error(reply.error());
      }
      return -1;
    }
    if (!std::holds_alternative<NbdkitReplyNames>(*reply)) {
      nbdkit_set_error(EIO);
      return -1;
    }

    auto &names_reply = std::get<NbdkitReplyNames>(*reply);
    for (const auto &name : names_reply.names) {
      if (nbdkit_add_export(exports, name.c_str(), "") == -1) {
        return -1;
      }
    }

    if (names_reply.seen_count < kListExportsBatchSize) {
      break;
    }
    if (names_reply.last_seen_name.empty()) {
      nbdkit_set_error(EIO);
      return -1;
    }

    start_kind = ReaddirStartKind::AfterName;
    start_name = names_reply.last_seen_name;
  }
  return 0;
}

static const char *fdbfs_default_export(int readonly, int is_tls) {
  (void)readonly;
  (void)is_tls;
  return kDefaultExport.data();
}

static const char *fdbfs_export_description(void *handle) {
  (void)handle;
  return "Default fdbfs export";
}

static void fdbfs_cleanup(void) {
  shut_it_down();
  if (g_fdbfs_runtime != nullptr) {
    g_fdbfs_runtime->stop_restartable();
  }
  g_runtime_owner.reset();
}

static int fdbfs_block_size(void *handle, uint32_t *minimum,
                            uint32_t *preferred, uint32_t *maximum) {
  (void)handle;
  *minimum = 1;
  *preferred = BLOCKSIZE;
  *maximum = 128 * 1024;
  return 0;
}

static struct nbdkit_plugin plugin = {
    .name = "fdbfs",
    .longname = "fdbfs nbdkit plugin",
    .version = "0",
    .description = "fdbfs backed storage, treats files as disk images",
    .load = fdbfs_load,
    .unload = fdbfs_unload,
    .config = fdbfs_config,
    .config_complete = fdbfs_config_complete,
    .config_help = "key_prefix=<prefix>  FoundationDB key prefix (default: FS)",
    .open = fdbfs_open,
    .close = fdbfs_close,
    .get_size = fdbfs_get_size,
    .can_write = fdbfs_can_write,
    .can_flush = fdbfs_can_flush,
    .is_rotational = fdbfs_is_rotational,
    .can_trim = fdbfs_can_trim,
    .errno_is_preserved = 0,
    .dump_plugin = fdbfs_dump_plugin,
    .can_zero = fdbfs_can_zero,
    .can_fua = fdbfs_can_fua,
    .pread = fdbfs_pread,
    .pwrite = fdbfs_pwrite,
    .flush = fdbfs_flush,
    .trim = fdbfs_trim,
    .zero = fdbfs_zero,
    .magic_config_key = nullptr,
    .can_multi_conn = fdbfs_can_multi_conn,
    .can_cache = fdbfs_can_cache,
    .can_fast_zero = fdbfs_can_fast_zero,
    .preconnect = fdbfs_preconnect,
    .get_ready = fdbfs_get_ready,
    .after_fork = fdbfs_after_fork,
    .list_exports = fdbfs_list_exports,
    .default_export = fdbfs_default_export,
    .export_description = fdbfs_export_description,
    .cleanup = fdbfs_cleanup,
    .block_size = fdbfs_block_size,
};

} // namespace

NBDKIT_REGISTER_PLUGIN(plugin)
