# fdbfs

needs a better name.

this isn't ready for prime time, yet.

# compiling

ensure you have the foundationdb client library, and protobuf
available for pkg-config to find.

for "precise" details, see the Travis CI configuration file, as it
is able to run the build (and right as soon as there are tests, those too!)

# backwards incompatibilities

you shouldn't be using this, let alone storing real data in it yet.

but as precommitment to the possibility that i'll take this software
seriously someday, i'll attempt to track any backwards incompatibilities
here.

* 2019-10-07 KV layout changed; fileblocks and dirents moved away from
  the inode records in KV-space. you could transform the old format into
  the new one programmatically if the filesystem was offline. no code
  exists for that. do a dump and restore.

# long termish TODO

* main priority: eliminate all bugs, implement all FUSE operations,
  full compatibility for user space code written against ext4/UFS/ZFS.

* mkfs, fsck, command line options
* coalesce multiple writes to the same kv pair
* read-only compatibility with FDB directory layer
* configurable compression
* configurable per-block ECC
* extract FS operations into non-FUSE specific library,
  while maintaining performance in FUSE case
* samba VFS module
* dokan support
