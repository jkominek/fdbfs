# Filesystem testing

https://stackoverflow.com/questions/21565865/filesystem-test-suites

https://github.com/billziss-gh/secfs.test

# Library references

at some point we need to actually hold inodes open when we're using
them, which means storing in-use records in FDB. applying every
increment/decrement to FDB is unnecessary. we should track it internally
and just change FDB when we go from 0 to not-0. that means we'll need
an in-memory dictionary from fuse_ino_t to counts.

* http://concurrencykit.org/ looks reasonable, brings along other
  valuable things, and is available as a debian package/shared library
