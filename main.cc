
#define FUSE_USE_VERSION 26
#include <fuse_lowlevel.h>
#define FDB_API_VERSION 610
#include <foundationdb/fdb_c.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <pthread.h>
#include <stdbool.h>

#include <string>
#include <memory>

#include "util.h"
#include "values.pb.h"

// will be filled out before operation begins
FDBDatabase *database;
uint8_t BLOCKBITS;
uint32_t BLOCKSIZE; // 1<<BLOCKBITS

/*************************************************************
 * setup
 *************************************************************/

extern "C" void fdbfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name);
extern "C" void fdbfs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern "C" void fdbfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
extern "C" void fdbfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
extern "C" void fdbfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
extern "C" void fdbfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev);
extern "C" void fdbfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode);
extern "C" void fdbfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name);
extern "C" void fdbfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name);
extern "C" void fdbfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname);
extern "C" void fdbfs_readlink(fuse_req_t req, fuse_ino_t ino);
extern "C" void fdbfs_symlink(fuse_req_t req, const char *link,
			      fuse_ino_t parent, const char *name);
extern "C" void fdbfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi);
extern "C" void fdbfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname);
extern "C" void fdbfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi);

/* These are our entry points for the operations. They'll set
 * up the appropriate inflight structure and make the initial
 * call to the issuer.
 */
static struct fuse_lowlevel_ops fdbfs_oper =
  {
    .lookup	= fdbfs_lookup,
    .getattr	= fdbfs_getattr,
    .readdir	= fdbfs_readdir,
    .open	= fdbfs_open,
    .read	= fdbfs_read,
    .mknod      = fdbfs_mknod,
    .mkdir      = fdbfs_mkdir,
    .unlink     = fdbfs_unlink,
    .rmdir      = fdbfs_rmdir,
    .link       = fdbfs_link,
    .readlink   = fdbfs_readlink,
    .symlink    = fdbfs_symlink,
    .setattr	= fdbfs_setattr,
    .rename     = fdbfs_rename,
    .write      = fdbfs_write,
  };

/* Purely to get the FoundationDB network stuff running in a
 * background thread. Passing fdb_run_network straight to
 * pthread_create kind of works, but let's pretend this will
 * be robust cross platform code someday.
 */
void *network_runner(void *ignore)
{
  if(fdb_run_network()) {
    ;
  }
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

  GOOGLE_PROTOBUF_VERIFY_VERSION;

  key_prefix.push_back('F');
  key_prefix.push_back('S');
  BLOCKBITS = 13;
  BLOCKSIZE = 1<<BLOCKBITS;
  
  if(fdb_select_api_version(610))
    return -1;
  if(fdb_setup_network())
    return -1;

  pthread_t network_thread;
  pthread_create(&network_thread, NULL, network_runner, NULL);

  if(fdb_create_database(NULL, &database))
    return -1;
	  
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

  fdb_database_destroy(database);
  err = fdb_stop_network();
  err = err || pthread_join( network_thread, NULL );
  return err;
}
