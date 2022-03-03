gen = generator(protoc, \
  output    : ['@BASENAME@.pb.cc', '@BASENAME@.pb.h'],
  arguments : ['--proto_path=@CURRENT_SOURCE_DIR@', '--cpp_out=@BUILD_DIR@', '@INPUT@'])
generated += gen.process(files('values.proto'))

project_source_files += files([
    'main.cc',
    'garbage_collector.cc',
    'liveness.cc',
    'util.cc',

    # fuse ops
#    'flock.cc',
    'flush.cc',
    'forget.cc',
    'getattr.cc',
    'getxattr.cc',
    'inflight.cc',
    'link.cc',
    'listxattr.cc',
    'lookup.cc',
    'mknod.cc',
    'open.cc',
    'read.cc',
    'readdir.cc',
    'readlink.cc',
    'release.cc',
    'removexattr.cc',
    'rename.cc',
    'setattr.cc',
    'setxattr.cc',
    'statfs.cc',
    'unlink.cc',
    'write.cc'
])

