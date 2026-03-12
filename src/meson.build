gen = generator(protoc, \
  output    : ['@BASENAME@.pb.cc', '@BASENAME@.pb.h'],
  arguments : ['--proto_path=@CURRENT_SOURCE_DIR@', '--cpp_out=@BUILD_DIR@', '@INPUT@'])
generated += gen.process(files('values.proto'))

project_source_files += files([
    'fuse/main.cc',
    'fuse/fdbfs_ops.cc',
    'generic/filehandle.cc',
    'generic/garbage_collector.cc',
    'generic/liveness.cc',
    'generic/util.cc',
    'generic/util_locks.cc',
    'generic/util_unlink.cc',
    'generic/inflight.cc',
])
