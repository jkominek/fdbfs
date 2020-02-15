
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
#include "garbage_collector.h"
#include "fdbfs_ops.h"

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
static struct fuse_lowlevel_ops fdbfs_oper =
  {
    .lookup	= fdbfs_lookup,
    .getattr	= fdbfs_getattr,
    .readdir	= fdbfs_readdir,
    .open	= fdbfs_open,
    .release	= fdbfs_release,
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
    .forget     = fdbfs_forget,
    .forget_multi= fdbfs_forget_multi,
    .statfs     = fdbfs_statfs,
    .getxattr   = fdbfs_getxattr,
    .setxattr   = fdbfs_setxattr,
    .removexattr= fdbfs_removexattr
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
  inode_key_length = pack_inode_key(0).size();
  fileblock_prefix_length = inode_key_length;
  // careful, we might make this variable
  fileblock_key_length = pack_fileblock_key(0, 0).size();
  dirent_prefix_length = pack_dentry_key(0, "").size();
  BLOCKBITS = 13;
  BLOCKSIZE = 1<<BLOCKBITS;

  // give us some initial space.
  lookup_counts.reserve(128);

  // this probably isn't the best way to produce 128 bits in
  // a std::vector, but, whatever.
  for(int i=0; i<16; i++) {
    inode_use_identifier.push_back(random() & 0xFF);
  }
  // TODO put our inode_use_identifier into the pid table somehow
  
  if(fdb_select_api_version(610))
    return -1;
  if(fdb_setup_network())
    return -1;

  pthread_t network_thread;
  pthread_create(&network_thread, NULL, network_runner, NULL);

  if(fdb_create_database(NULL, &database))
    return -1;

  pthread_t gc_thread;
  pthread_create(&gc_thread, NULL, garbage_scanner, NULL);

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
  database = NULL;
  err = fdb_stop_network();
  err = err || pthread_join( network_thread, NULL );
  return err;
}
