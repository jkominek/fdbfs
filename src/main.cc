
#define FUSE_USE_VERSION 35
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <memory>
#include <string>

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
    .readdir = fdbfs_readdir,
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
  // per the docs, transactions should be kept under 1MB.
  // let's stay well below that.
  if (conn->max_write > (1 << 17))
    conn->max_write = 1 << 17;
  // TODO maybe set this to the number of storage servers, or
  // half that, or somethimg. using 4 at the moment, to be
  // interesting.
  conn->max_background = 4;
  // TODO some intelligent way to set this?
  conn->congestion_threshold = 0;
}

pthread_t network_thread;
bool network_thread_created = false;
pthread_t gc_thread;

void fdbfs_destroy(void *userdata) {
  // TODO bring gc_thread to a clean halt
  // TODO check return codes and do... what?
  fdb_database_destroy(database);
  if (fdb_stop_network()) {
    ;
  }
  pthread_join(network_thread, NULL);
  exit(0);
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

/* main is a mess.
 * TODO structure main more clearly, perhaps break things out
 */
int main(int argc, char *argv[]) {
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
  struct fuse_session *se;
  struct fuse_cmdline_opts opts;
  struct fuse_loop_config config;
  int err = -1;
  bool fdb_network_setup_done = false;

  GOOGLE_PROTOBUF_VERIFY_VERSION;

  // TODO key_prefix needs to be configurable, both
  // to prevent key space conflicts, and to allow multiple
  // filesystems in one database.
  key_prefix.push_back('F');
  key_prefix.push_back('S');
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

  // TODO inode_use_identifier needs to be unified with the
  // liveness manager's pid. probably by just throwing it away
  // and switching the one use to the pid value.
  for (int i = 0; i < 16; i++) {
    inode_use_identifier.push_back(random() & 0xFF);
  }

  if (fuse_parse_cmdline(&args, &opts) != 0)
    return 1;

  if (opts.show_help) {
    printf("usage: %s [options] <mountpoint>\n\n", argv[0]);
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
    fuse_session_destroy(se);
    err = 1;
    goto shutdown_session;
  }
  if (fuse_session_mount(se, opts.mountpoint) != 0) {
    fuse_remove_signal_handlers(se);
    fuse_session_destroy(se);
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
    goto shutdown_unmount;
  }

  if (fdb_setup_network()) {
    err = 1;
    goto shutdown_unmount;
  }
  fdb_network_setup_done = true;

  if (pthread_create(&network_thread, NULL, network_runner, NULL))
    goto shutdown_fdb;
  network_thread_created = true;

  if (fdb_create_database(NULL, &database)) {
    goto shutdown_fdb;
  }

  pthread_create(&gc_thread, NULL, garbage_scanner, NULL);

  if (opts.singlethread) {
    err = fuse_session_loop(se);
  } else {
    config.clone_fd = opts.clone_fd;
    config.max_idle_threads = opts.max_idle_threads;
    err = fuse_session_loop_mt(se, &config);
  }

shutdown_unmount:
  fuse_session_unmount(se);
  terminate_liveness();
shutdown_handlers:
  fuse_remove_signal_handlers(se);
shutdown_session:
  fuse_session_destroy(se);

shutdown_args:
  fuse_opt_free_args(&args);

shutdown_fdb:
  if (fdb_network_setup_done)
    err = err || fdb_stop_network();
  if (network_thread_created)
    err = err || pthread_join(network_thread, NULL);

  return err;
}
