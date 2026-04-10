gen = generator(
    protoc,
    output: ['@BASENAME@.pb.cc', '@BASENAME@.pb.h'],
    arguments: [
        '--proto_path=@CURRENT_SOURCE_DIR@',
        '--cpp_out=@BUILD_DIR@',
        '@INPUT@',
    ],
)
generated += gen.process(files('values.proto'))

fuse_source_files += files(
    'fuse/directoryhandle.cc',
    'fuse/fdbfs_ops.cc',
    'fuse/filehandle.cc',
    'fuse/inflight_instantiations.cc',
    'fuse/main.cc',
    'fuse/util_fuse.cc',
)

generic_source_files += files(
    'generic/fdb_service.cc',
    'generic/fdbfs_runtime.cc',
    'generic/garbage_collector.cc',
    'generic/liveness.cc',
    'generic/oplog.cc',
    'generic/util.cc',
    'generic/util_locks.cc',
    'generic/util_unlink.cc',
)

sqlite3_source_files += files(
    'sqlite3/io_plan.cc',
    'sqlite3/lib.cc',
    'sqlite3/sqlite_ops.cc',
    'sqlite3/util_sqlite.cc',
)

nbdkit_source_files += files(
    'nbdkit/plugin.cc',
)

if have_fuse
    project_source_files += fuse_source_files + generic_source_files
endif

utils_source_files = files(
    'utils/fsck.fdbfs.cc',
    'utils/mkfs.fdbfs.cc',
)

utils_runtime_source_files = files(
    'generic/fdb_service.cc',
    'generic/fdbfs_runtime.cc',
    'generic/liveness.cc',
    'generic/util.cc',
)
