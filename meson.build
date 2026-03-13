project('fdbfs', 'cpp', default_options: ['cpp_std=c++23'])

cc = meson.get_compiler('cpp')
add_project_arguments(cc.get_supported_arguments(['-Wall']), language: 'cpp')

protoc = find_program('protoc', required : true)

deps = [
  dependency('fuse3'),
  dependency('libzstd'),
  dependency('protobuf'),
  dependency('foundationdb-client'),
  dependency('nlohmann_json'),
]

project_includes = include_directories(
  'src',
  'src/generic',
  'src/fuse',
  'tests',
  'tests/fuse_integrated',
  'thirdparty/concurrentqueue'
)

project_source_files = []
generated = []

subdir('src')

fs_exe = executable('fs', project_source_files, generated,
  dependencies: deps,
  include_directories: project_includes,
)

# -------------------------
# Tests (Catch2 v3 installed)
# -------------------------

catch2_dep = dependency('catch2-with-main', version : '>=3.4.0', required : true)

pure_helpers_sources = files(
  'src/generic/util.cc',
  'tests/test_support.cc',
  'tests/test_pure_helpers.cc',
  'tests/test_support.cc'
)

pure_helpers_exe = executable(
  'fdbfs_pure_helpers_bin',
  pure_helpers_sources,
  generated,
  dependencies : [catch2_dep] + deps,
  include_directories : project_includes,
)

test(
  'fdbfs_pure_helpers',
  pure_helpers_exe,
  timeout : 60,
  is_parallel : false,
)

test_sources = files(
  'tests/fuse_integrated/test_dir_nlink.cc',
  'tests/fuse_integrated/test_file_io.cc',
  'tests/fuse_integrated/test_getattr.cc',
  'tests/fuse_integrated/test_link.cc',
  'tests/fuse_integrated/test_mkdir.cc',
  'tests/fuse_integrated/test_mknod.cc',
  'tests/fuse_integrated/test_mount.cc',
  'tests/fuse_integrated/test_open.cc',
  'tests/fuse_integrated/test_fsync.cc',
  'tests/fuse_integrated/test_posix_locks.cc',
  'tests/fuse_integrated/test_read.cc',
  'tests/fuse_integrated/test_readdir.cc',
  'tests/fuse_integrated/test_readlink.cc',
  'tests/fuse_integrated/test_rename.cc',
  'tests/fuse_integrated/test_rmdir.cc',
  'tests/fuse_integrated/test_setattr.cc',
  'tests/test_support.cc',
  'tests/fuse_integrated/test_unlink.cc',
  'tests/fuse_integrated/test_write.cc',
  'tests/fuse_integrated/test_xattr.cc',
)

# If your tests need the same deps as the filesystem client libraries, add `deps` too.
# If tests only exec ./fs and do syscalls, you can omit `deps`.
test_exe = executable(
  'fdbfs_tests',
  test_sources,
  dependencies : [catch2_dep],
  include_directories : project_includes,
  # Useful while developing integration tests:
  # cpp_args : cc.get_supported_arguments(['-O0', '-g']),
)

test(
  'fdbfs_hostfs_validation',
  test_exe,
  env : [
    'FDBFS_TEST_BACKEND=host',
    'FDBFS_TEST_MATRIX=empty',
  ],
  timeout : 90,
  is_parallel : false,
)

# Make ./fs available to the test at a stable path.
# Meson runs tests from the build dir, so you can reference it as:
#   fs_exe = join_paths(meson.current_build_dir(), 'fs')
#
# Pass it via an environment variable so your tests don't depend on CWD assumptions.
test(
  'fdbfs_fuse_integrated_empty',
  test_exe,
  env : [
    'FDBFS_FS_EXE=' + fs_exe.full_path(),
    'FDBFS_SOURCE_DIR=' + meson.project_source_root(),
    'FDBFS_TEST_MATRIX=empty',
  ],
  timeout : 240,
  is_parallel : false,
)

test(
  'fdbfs_fuse_integrated_seeded',
  test_exe,
  env : [
    'FDBFS_FS_EXE=' + fs_exe.full_path(),
    'FDBFS_SOURCE_DIR=' + meson.project_source_root(),
    'FDBFS_TEST_MATRIX=seeded',
  ],
  timeout : 420,
  is_parallel : false,
)

# Optional: a quick “smoke” test label so you can run subset:
# meson test --suite smoke
# test('fdbfs_smoke', test_exe, suite : ['smoke'], env : [...])
