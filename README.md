# fdbfs

needs a better name.

this isn't ready for prime time, yet.

# compiling

ensure you have the foundationdb client library, and protobuf
available for pkg-config to find.

run make. cross your fingers.

# backwards incompatibilities

you shouldn't be using this, let alone storing real data in it yet.

but as precommitment to the possibility that i'll take this software
seriously someday, i'll attempt to track any backwards incompatibilities
here.

* 2019-10-07 KV layout changed; fileblocks and dirents moved away from
  the inode records in KV-space. you could transform the old format into
  the new one programmatically if the filesystem was offline. no code
  exists for that. do a dumpp and restore.
