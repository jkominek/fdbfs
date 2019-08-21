
#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 600
#include <foundationdb/fdb_c.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>

// will be filled out before operation begins
FDBDatabase *database;
char *kp;
int kplen;
uint8_t BLOCKBITS;
uint32_t BLOCKSIZE; // 1<<BLOCKBITS

/*************************************************************
 * setup
 *************************************************************/

extern void fdbfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void fdbfs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void fdbfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi);
extern void fdbfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
extern void fdbfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern void fdbfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
extern void fdbfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);
extern void fdbfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode);
extern void fdbfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void fdbfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev);
extern void fdbfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name);
extern void fdbfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname);
extern void fdbfs_readlink(fuse_req_t req, fuse_ino_t ino);
extern void fdbfs_symlink(fuse_req_t req, const char *link,
			  fuse_ino_t parent, const char *name);
extern void fdbfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname);

/* These are our entry points for the operations. They'll set
 * up the appropriate inflight structure and make the initial
 * call to the issuer.
 */
static struct fuse_lowlevel_ops fdbfs_oper =
  {
    .lookup	= fdbfs_lookup,
    .getattr	= fdbfs_getattr,
    .setattr	= fdbfs_setattr,
    .readdir	= fdbfs_readdir,
    .open	= fdbfs_open,
    .read	= fdbfs_read,
    .write      = fdbfs_write,
    .mkdir      = fdbfs_mkdir,
    .rmdir      = fdbfs_rmdir,
    .mknod      = fdbfs_mknod,
    .unlink     = fdbfs_unlink,
    .link       = fdbfs_link,
    .readlink   = fdbfs_readlink,
    .symlink    = fdbfs_symlink,
    .rename     = fdbfs_rename,
  };

/* Purely to get the FoundationDB network stuff running in a
 * background thread. Passing fdb_run_network straight to
 * pthread_create kind of works, but let's pretend this will
 * be robust cross platform code someday.
 */
void *network_runner(void *ignore)
{
  fdb_run_network();
  return NULL;
}

/* main is a mess.
 * i don't pretend any of this is reasonable.
 */
int main(int argc, char *argv[])
{
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
  struct fuse_chan *ch;
  char *mountpoint;
  int err = -1;

  kp = "FS";
  kplen = 2;
  BLOCKBITS = 13;
  BLOCKSIZE = 1<<BLOCKBITS;
  
  if(fdb_select_api_version(600))
    goto fail_nothing_setup;
  if(fdb_setup_network())
    goto fail_nothing_setup;

  pthread_t network_thread;
  pthread_create(&network_thread, NULL, network_runner, NULL);
	
  FDBFuture *f_cluster = fdb_create_cluster(NULL);
  if(fdb_future_block_until_ready(f_cluster))
    goto fail_fdb_network_running;
  if(fdb_future_get_error(f_cluster))
    goto fail_fdb_network_running;
  FDBCluster *cluster;
  if(fdb_future_get_cluster(f_cluster, &cluster))
    goto fail_fdb_network_running;
  fdb_future_destroy(f_cluster);

  FDBFuture *f_database = fdb_cluster_create_database(cluster, (const uint8_t *)"DB", 2);
  if(fdb_future_block_until_ready(f_database))
    goto fail_fdb_network_running;
  if(fdb_future_get_error(f_database))
    goto fail_fdb_network_running;
  if(fdb_future_get_database(f_database, &database))
    goto fail_fdb_network_running;
  fdb_future_destroy(f_database);
	  
  if ((fuse_parse_cmdline(&args, &mountpoint, NULL, NULL) != -1) &&
      ((ch = fuse_mount(mountpoint, &args)) != NULL))
    {
      struct fuse_session *se;
      
      se = fuse_lowlevel_new(&args, &fdbfs_oper,
			     sizeof(fdbfs_oper), NULL);
      if (se != NULL)
	{
	  if (fuse_set_signal_handlers(se) != -1)
	    {
	      fuse_session_add_chan(se, ch);
	      err = fuse_session_loop(se);
	      fuse_remove_signal_handlers(se);
	      fuse_session_remove_chan(ch);
	    }
	  fuse_session_destroy(se);
	}
      fuse_unmount(mountpoint, ch);
    }
  fuse_opt_free_args(&args);
  
 fail_fdb_network_running:
  err = fdb_stop_network();
  err = err || pthread_join( network_thread, NULL );
 fail_nothing_setup:
  return err ? 1 : 0;
}
