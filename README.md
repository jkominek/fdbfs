[![Build Status](https://travis-ci.org/jkominek/fdbfs.svg?branch=master)](https://travis-ci.org/jkominek/fdbfs)
[![GitHub license](https://img.shields.io/badge/license-ISC-blue.svg)](https://raw.githubusercontent.com/jkominek/fdbfs/master/LICENSE)

# fdbfs

needs a better name.

this isn't ready for prime time, yet.

# compiling

ensure you have the foundationdb client library, and protobuf
available for pkg-config to find.

for "precise" details, see the Github actions configuration file, as it
is able to run the build and tests.

# backwards incompatibilities

you shouldn't be using this, let alone storing real data in it yet.

* 2026 heavy development; backwards incompatibility probably lost.

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
