gen = generator(protoc, \
  output    : ['@BASENAME@.pb.cc', '@BASENAME@.pb.h'],
  arguments : ['--proto_path=@CURRENT_SOURCE_DIR@', '--cpp_out=@BUILD_DIR@', '@INPUT@'])
generated += gen.process(files('values.proto'))

project_source_files += files([
    'fuse/main.cc',
    'generic/filehandle.cc',
    'generic/garbage_collector.cc',
    'generic/liveness.cc',
    'generic/util.cc',
    'generic/util_locks.cc',
    'generic/util_unlink.cc',

    # fuse ops
    'generic/flush.cc',
    'generic/fsync.cc',
    'generic/forget.cc',
    'generic/getattr.cc',
    'generic/getxattr.cc',
    'generic/inflight.cc',
    'generic/link.cc',
    'generic/listxattr.cc',
    'generic/lookup.cc',
    'generic/mknod.cc',
    'generic/open.cc',
    'generic/read.cc',
    'generic/readdir.cc',
    'generic/readdirplus.cc',
    'generic/readlink.cc',
    'generic/release.cc',
    'generic/removexattr.cc',
    'generic/rename.cc',
    'generic/posix_locks.cc',
    'generic/setattr.cc',
    'generic/setxattr.cc',
    'generic/statfs.cc',
    'generic/unlink.cc',
    'generic/write.cc'
])
