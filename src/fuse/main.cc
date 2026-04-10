
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <execinfo.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "fuse/fdbfs_ops.h"
#include "generic/fdb_service.h"
#include "generic/fdbfs_runtime.h"
#include "generic/garbage_collector.h"
#include "generic/liveness.h"
#include "generic/util.h"
#include "generic/util_locks.h"
#include "values.pb.h"

uint8_t BLOCKBITS;
uint32_t BLOCKSIZE; // 1<<BLOCKBITS

/*************************************************************
 * setup
 *************************************************************/

/* These are our entry points for the operations. They'll set
 * up the appropriate inflight structure and make the initial
 * call to the issuer.
 */
static struct fuse_lowlevel_ops fdbfs_oper = {
    .init = fdbfs_init,
    .destroy = fdbfs_destroy,
    .lookup = fdbfs_lookup,
    .forget = fdbfs_forget,
    .getattr = fdbfs_getattr,
    .setattr = fdbfs_setattr,
    .readlink = fdbfs_readlink,
    .mknod = fdbfs_mknod,
    .mkdir = fdbfs_mkdir,
    .unlink = fdbfs_unlink,
    .rmdir = fdbfs_rmdir,
    .symlink = fdbfs_symlink,
    .rename = fdbfs_rename,
    .link = fdbfs_link,
    .open = fdbfs_open,
    .read = fdbfs_read,
    .write = fdbfs_write,
    .flush = fdbfs_flush,
    .release = fdbfs_release,
    .fsync = fdbfs_fsync,
    .opendir = fdbfs_opendir,
    .readdir = fdbfs_readdir,
    .releasedir = fdbfs_releasedir,
    .fsyncdir = fdbfs_fsyncdir,
    .statfs = fdbfs_statfs,
    .setxattr = fdbfs_setxattr,
    .getxattr = fdbfs_getxattr,
    .listxattr = fdbfs_listxattr,
    .removexattr = fdbfs_removexattr,
    .getlk = fdbfs_getlk,
    .setlk = fdbfs_setlk,
    .forget_multi = fdbfs_forget_multi,
    .readdirplus = fdbfs_readdirplus,
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 17)
    .tmpfile = fdbfs_tmpfile,
#endif
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 18)
    .statx = fdbfs_statx,
#endif
};

namespace {

class FuseArgsGuard {
public:
  explicit FuseArgsGuard(struct fuse_args &args) : args_(args) {}
  ~FuseArgsGuard() { fuse_opt_free_args(&args_); }

private:
  struct fuse_args &args_;
};

class FuseSessionService {
public:
  FuseSessionService(struct fuse_args &args, const char *mountpoint,
                     const fuse_lowlevel_ops &ops, size_t ops_size) {
    se_ = fuse_session_new(&args, &ops, ops_size, NULL);
    if (se_ == NULL) {
      throw std::runtime_error("fuse_session_new failed");
    }

    if (fuse_set_signal_handlers(se_) != 0) {
      throw std::runtime_error("fuse_set_signal_handlers failed");
    }
    handlers_installed_ = true;

    if (fuse_session_mount(se_, mountpoint) != 0) {
      throw std::runtime_error("fuse_session_mount failed");
    }
    mounted_ = true;
  }

  ~FuseSessionService() {
    if (mounted_) {
      fuse_session_unmount(se_);
    }
    if (handlers_installed_) {
      fuse_remove_signal_handlers(se_);
    }
    if (se_ != nullptr) {
      fuse_session_destroy(se_);
      se_ = nullptr;
    }
  }

  FuseSessionService(const FuseSessionService &) = delete;
  FuseSessionService &operator=(const FuseSessionService &) = delete;
  FuseSessionService(FuseSessionService &&) = delete;
  FuseSessionService &operator=(FuseSessionService &&) = delete;

  [[nodiscard]] fuse_session *get() const { return se_; }

private:
  fuse_session *se_ = nullptr;
  bool mounted_ = false;
  bool handlers_installed_ = false;
};

} // namespace

struct fdbfs_options {
  const char *key_prefix;
  const char *cluster_file;
  int buggify;
};

#define FDBFS_OPT(t, p) {t, offsetof(struct fdbfs_options, p), 1}

static const struct fuse_opt fdbfs_option_spec[] = {
    FDBFS_OPT("--key-prefix=%s", key_prefix),
    FDBFS_OPT("-k %s", key_prefix),
    FDBFS_OPT("--cluster-file=%s", cluster_file),
    FDBFS_OPT("--buggify", buggify),
    FUSE_OPT_END,
};

static void fdbfs_crash_signal_handler(int sig) {
  const char *name = nullptr;
  switch (sig) {
  case SIGABRT:
    name = "SIGABRT";
    break;
  case SIGSEGV:
    name = "SIGSEGV";
    break;
  default:
    name = "UNKNOWN";
    break;
  }

  dprintf(STDERR_FILENO, "fdbfs fatal signal %d (%s)\n", sig, name);
  void *frames[128];
  const int n = backtrace(frames, static_cast<int>(std::size(frames)));
  backtrace_symbols_fd(frames, n, STDERR_FILENO);
  _exit(128 + sig);
}

static void install_crash_signal_handlers() {
  struct sigaction sa{};
  sa.sa_handler = fdbfs_crash_signal_handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = SA_RESETHAND;
  (void)sigaction(SIGABRT, &sa, nullptr);
  (void)sigaction(SIGSEGV, &sa, nullptr);
}

/* main is a mess.
 * TODO structure main more clearly, perhaps break things out
 */
int main(int argc, char *argv[]) {
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
  FuseArgsGuard args_guard(args);
  struct fuse_session *se;
  struct fuse_cmdline_opts opts;
  struct fuse_loop_config config;
  struct fdbfs_options fdbfs_opts = {
      .key_prefix = NULL, .cluster_file = NULL, .buggify = 0};
  int err = 1;

  setvbuf(stderr, nullptr, _IONBF, 0);
  install_crash_signal_handlers();

  GOOGLE_PROTOBUF_VERIFY_VERSION;

  if (fuse_opt_parse(&args, &fdbfs_opts, fdbfs_option_spec, NULL) != 0) {
    return 1;
  }
  const char *selected_key_prefix =
      (fdbfs_opts.key_prefix == NULL) ? "FS" : fdbfs_opts.key_prefix;
  if (selected_key_prefix[0] == '\0') {
    fprintf(stderr, "fdbfs: --key-prefix must be non-empty\n");
    return 1;
  }
  if ((fdbfs_opts.cluster_file != NULL) && (fdbfs_opts.cluster_file[0] == '\0')) {
    fprintf(stderr, "fdbfs: --cluster-file must be non-empty\n");
    return 1;
  }

  key_prefix.clear();
  key_prefix.insert(key_prefix.end(),
                    reinterpret_cast<const uint8_t *>(selected_key_prefix),
                    reinterpret_cast<const uint8_t *>(selected_key_prefix) +
                        strlen(selected_key_prefix));

  inode_key_length = pack_inode_key(0).size();
  fileblock_prefix_length = inode_key_length;
  // careful, we might make this variable
  fileblock_key_length = pack_fileblock_key(0, 0).size();
  dirent_prefix_length = pack_dentry_key(0, "").size();
  // TODO should be stored in the filesystem metadata and
  // pulled from there.
  BLOCKBITS = 13;
  BLOCKSIZE = 1 << BLOCKBITS;

  // give us some initial space.
  lookup_counts.reserve(128);

  fdbfs_set_thread_name("main");

  if (fuse_parse_cmdline(&args, &opts) != 0)
    return 1;

  if (opts.show_help) {
    printf("usage: %s [options] <mountpoint>\n\n", argv[0]);
    printf("fdbfs options:\n");
    printf("    --key-prefix=STR, -k STR   FoundationDB key prefix "
           "(default: FS)\n");
    printf("    --cluster-file=PATH        FoundationDB cluster file path\n");
    printf("    --buggify                  Enable FoundationDB client "
           "buggify\n\n");
    fuse_cmdline_help();
    fuse_lowlevel_help();
    return 0;
  } else if (opts.show_version) {
    printf("FUSE library version %s\n", fuse_pkgversion());
    fuse_lowlevel_version();
    return 0;
  }

  if (opts.mountpoint == NULL) {
    printf("usage: %s [options] <mountpoint>\n", argv[0]);
    printf("       %s --help\n", argv[0]);
    return 1;
  }
  try {
    std::unique_ptr<FdbfsRuntime> runtime_owner;
    fdbfs_set_lock_manager_service(nullptr);
    FuseSessionService session(args, opts.mountpoint, fdbfs_oper,
                               sizeof(fdbfs_oper));
    se = session.get();

    // This appears to lock things up
    /*
    if(fuse_daemonize(opts.foreground)) {
      throw std::runtime_error("fuse_daemonize failed");
    }
    */

    runtime_owner = std::make_unique<FdbfsRuntime>();
    runtime_owner->add_persistent<FdbService>([&fdbfs_opts]() {
      return std::make_unique<FdbService>(fdbfs_opts.cluster_file,
                                          fdbfs_opts.buggify != 0);
    });
    runtime_owner->add_restartable<LivenessService>([se]() {
      return std::make_unique<LivenessService>(
          [se]() { fuse_session_exit(se); });
    });
    runtime_owner->add_restartable<GarbageCollectorService>(
        []() { return std::make_unique<GarbageCollectorService>(); });
    runtime_owner->add_restartable<LockManagerService>(
        []() { return std::make_unique<LockManagerService>(); });
    g_fdbfs_runtime = runtime_owner.get();
    runtime_owner->start_all();
    fdbfs_set_lock_manager_service(g_fdbfs_runtime->get<LockManagerService>());

    if (opts.singlethread) {
      err = fuse_session_loop(se);
    } else {
      config.clone_fd = opts.clone_fd;
      config.max_idle_threads = opts.max_idle_threads;
      err = fuse_session_loop_mt(se, &config);
    }

    fdbfs_set_lock_manager_service(nullptr);
    shut_it_down();
    if (g_fdbfs_runtime != nullptr) {
      g_fdbfs_runtime->stop_restartable();
    }
    runtime_owner.reset();
  } catch (const std::exception &e) {
    fdbfs_set_lock_manager_service(nullptr);
    shut_it_down();
    fprintf(stderr, "fdbfs startup/shutdown error: %s\n", e.what());
    err = 1;
  }

  return err;
}
