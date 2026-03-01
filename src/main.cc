
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <stddef.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <memory>
#include <string>
#include <thread>

#include "fdbfs_ops.h"
#include "garbage_collector.h"
#include "liveness.h"
#include "util.h"
#include "values.pb.h"

// will be filled out before operation begins
FDBDatabase *database;
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
    .readdir = fdbfs_readdir,
    .fsyncdir = fdbfs_fsyncdir,
    .statfs = fdbfs_statfs,
    .setxattr = fdbfs_setxattr,
    .getxattr = fdbfs_getxattr,
    .listxattr = fdbfs_listxattr,
    .removexattr = fdbfs_removexattr,
    .forget_multi = fdbfs_forget_multi,
    //    .flock      = fdbfs_flock
};

void fdbfs_init(void *userdata, struct fuse_conn_info *conn) {
  /*
  printf("max_write %i\n", conn->max_write);
  printf("max_read  %i\n", conn->max_read);
  printf("max_readahead %i\n", conn->max_readahead);
  printf("max_background %i\n", conn->max_background);
  printf("congestion_threshold %i\n", conn->congestion_threshold);
  printf("capable %x\n", conn->capable);
  printf("want    %x\n", conn->want);
  */
  // transactions have to finish in under 5 seconds, so unless
  // we get clever and start splitting our reads across transactions
  // (which we're not currently set up for) then we need a limit
  // on the size of reads
  // 1. this is probably conservative
  // 2. docs make it sound like this might also need to be set as
  //    a command line option.
  if (conn->max_read > 1024 * 1024)
    conn->max_read = 1024 * 1024;
  // per the docs, (write) transactions should be kept under 1MB.
  // let's stay well below that.
  if (conn->max_write > 128 * 1024)
    conn->max_write = 128 * 1024;
  // these largely deal with our relationship with the kernel, and
  // could probably be rather large
  conn->max_background = 256;
  conn->congestion_threshold = 192;
#if FUSE_VERSION >= 317
  if (conn->capable_ext & FUSE_CAP_ASYNC_DIO) {
    fuse_set_feature_flag(conn, FUSE_CAP_ASYNC_DIO);
  }
#endif
}

pthread_t network_thread;
bool network_thread_created = false;

void fdbfs_destroy(void *userdata) {
  // no-op. main takes care of everything when the session loop ends.
}

/* Purely to get the FoundationDB network stuff running in a
 * background thread. Passing fdb_run_network straight to
 * pthread_create kind of works, but let's pretend this will
 * be robust cross platform code someday.
 */
void *network_runner(void *ignore) {
  // TODO capture the return code and do something
  if (fdb_run_network()) {
    ;
  }
  return NULL;
}

struct fdbfs_options {
  const char *key_prefix;
  int buggify;
};

#define FDBFS_OPT(t, p) { t, offsetof(struct fdbfs_options, p), 1 }

static const struct fuse_opt fdbfs_option_spec[] = {
    FDBFS_OPT("--key-prefix=%s", key_prefix),
    FDBFS_OPT("-k %s", key_prefix),
    FDBFS_OPT("--buggify", buggify),
    FUSE_OPT_END,
};

/* main is a mess.
 * TODO structure main more clearly, perhaps break things out
 */
int main(int argc, char *argv[]) {
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
  struct fuse_session *se;
  struct fuse_cmdline_opts opts;
  struct fuse_loop_config config;
  struct fdbfs_options fdbfs_opts = {.key_prefix = NULL, .buggify = 0};
  int err = -1;
  bool fdb_network_setup_done = false;

  GOOGLE_PROTOBUF_VERIFY_VERSION;

  if (fuse_opt_parse(&args, &fdbfs_opts, fdbfs_option_spec, NULL) != 0) {
    return 1;
  }
  const char *selected_key_prefix =
      (fdbfs_opts.key_prefix == NULL) ? "FS" : fdbfs_opts.key_prefix;
  if (selected_key_prefix[0] == '\0') {
    fprintf(stderr, "fdbfs: --key-prefix must be non-empty\n");
    err = 1;
    goto shutdown_args;
  }

  key_prefix.clear();
  key_prefix.insert(
      key_prefix.end(),
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

  if (fuse_parse_cmdline(&args, &opts) != 0)
    return 1;

  if (opts.show_help) {
    printf("usage: %s [options] <mountpoint>\n\n", argv[0]);
    printf("fdbfs options:\n");
    printf("    --key-prefix=STR, -k STR   FoundationDB key prefix "
           "(default: FS)\n");
    printf(
        "    --buggify                  Enable FoundationDB client buggify\n\n");
    fuse_cmdline_help();
    fuse_lowlevel_help();
    err = 0;
    goto shutdown_args;
  } else if (opts.show_version) {
    printf("FUSE library version %s\n", fuse_pkgversion());
    fuse_lowlevel_version();
    err = 0;
    goto shutdown_args;
  }

  if (opts.mountpoint == NULL) {
    printf("usage: %s [options] <mountpoint>\n", argv[0]);
    printf("       %s --help\n", argv[0]);
    err = 1;
    goto shutdown_args;
  }

  se = fuse_session_new(&args, &fdbfs_oper, sizeof(fdbfs_oper), NULL);
  if (se == NULL) {
    err = 1;
    goto shutdown_args;
  }
  if (fuse_set_signal_handlers(se) != 0) {
    err = 1;
    goto shutdown_session;
  }
  if (fuse_session_mount(se, opts.mountpoint) != 0) {
    err = 1;
    goto shutdown_handlers;
  }

  // This appears to lock things up
  /*
  if(fuse_daemonize(opts.foreground)) {
    err = 1;
    goto shutdown_handlers;
  }
  */

  if (fdb_select_api_version(FDB_API_VERSION)) {
    err = 1;
    goto unmount_session;
  }

  if (fdbfs_opts.buggify &&
      fdb_network_set_option(FDB_NET_OPTION_CLIENT_BUGGIFY_ENABLE, NULL, 0)) {
    err = 1;
    goto unmount_session;
  }

  if (fdb_setup_network()) {
    err = 1;
    goto shutdown_fdb;
  }
  fdb_network_setup_done = true;

  if (pthread_create(&network_thread, NULL, network_runner, NULL)) {
    err = 1;
    goto shutdown_fdb;
  }
  network_thread_created = true;

  if (fdb_create_database(NULL, &database)) {
    database = NULL; // it failed, contents of the pointer are unspecified
    goto shutdown_fdb;
  }

  if (start_liveness(se)) {
    err = 1;
    goto shutdown_fdb;
  }

  if (start_gc()) {
    err = 1;
    goto shutdown_liveness;
  }

  if (opts.singlethread) {
    err = fuse_session_loop(se);
  } else {
    config.clone_fd = opts.clone_fd;
    config.max_idle_threads = opts.max_idle_threads;
    err = fuse_session_loop_mt(se, &config);
  }

  terminate_gc();
shutdown_liveness:
  terminate_liveness();

shutdown_fdb:
  if (database)
    fdb_database_destroy(database);

  if (fdb_network_setup_done)
    err = err || fdb_stop_network();
  if (network_thread_created)
    err = err || pthread_join(network_thread, NULL);

unmount_session:
  fuse_session_unmount(se);
shutdown_handlers:
  fuse_remove_signal_handlers(se);
shutdown_session:
  fuse_session_destroy(se);
shutdown_args:
  fuse_opt_free_args(&args);

  return err;
}
