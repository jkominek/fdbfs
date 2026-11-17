[![Build Status](https://github.com/jkominek/fdbfs/actions/workflows/cpp.yml/badge.svg)](https://github.com/jkominek/fdbfs/actions/workflows/cpp.yml)
[![GitHub license](https://img.shields.io/badge/license-ISC-blue.svg)](https://raw.githubusercontent.com/jkominek/fdbfs/master/LICENSE)

# fdbfs

this has evolved into a composable set of filesystem operations on top of foundationdb.
it comes with layers on top of that for:

* a FUSE filesystem
* a SQLite VFS layer
* Network Block Device (via nbdkit)

there's a halfway decent test suite which attempts to exercise the code at multiple levels
of abstraction, including the various layers.

# compiling

see the Github actions configuration file, as it is able to run the build and tests.

# backwards incompatibilities

i can't quite guarantee incompatibilities won't be introduced, but the
keyspace layout seems to be working okay so far.

# long termish TODO

* main priorities
  * eliminate all bugs
  * implement all FUSE operations,
  * full compatibility for user space code written against ext4/UFS/ZFS.
  * tests. and running under valgrind during tests.

everything else is roughly descending order:

* mkfs
* fsck and command line options for the FS itself
* coalesce multiple writes to the same kv pair
* read-only compatibility with FDB directory layer
* configurable compression
* configurable per-block ECC
* extract FS operations into non-FUSE specific library,
  while maintaining performance in FUSE case
  * samba VFS module
  * dokan support
  * user space LD_PRELOAD'able library?
