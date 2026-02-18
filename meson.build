project('fdbfs', 'cpp',
  default_options : ['cpp_std=c++23']
)

cc = meson.get_compiler('cpp')
add_project_arguments(
  cc.get_supported_arguments(['-Wall']),
  language : 'cpp'
)

protoc = find_program('protoc', required : true)

deps = [
  dependency('fuse3'),
  dependency('libzstd'),
  dependency('protobuf'),
  dependency('foundationdb-client'),
  dependency('nlohmann_json'),
]

project_source_files = []
generated = []

subdir('src')

fs_exe = executable(
  'fs',
  project_source_files,
  generated,
  dependencies : deps
)

# -------------------------
# Tests (Catch2 v3 installed)
# -------------------------

catch2_dep = dependency('catch2-with-main', version : '>=3.4.0', required : true)

pure_helpers_sources = files(
  'tests/test_pure_helpers.cc',
  'src/util.cc',
)

pure_helpers_exe = executable(
  'fdbfs_pure_helpers_bin',
  pure_helpers_sources,
  generated,
  dependencies : [catch2_dep] + deps,
  include_directories : include_directories('src'),
)

test(
  'fdbfs_pure_helpers',
  pure_helpers_exe,
  timeout : 60,
  is_parallel : false,
)

test_sources = files(
  'tests/test_support.cc',
  'tests/test_file_io.cc',
  'tests/test_getattr.cc',
  'tests/test_link.cc',
  'tests/test_mknod.cc',
  'tests/test_mount.cc',
  'tests/test_mkdir.cc',
  'tests/test_open.cc',
  'tests/test_readdir.cc',
  'tests/test_rmdir.cc',
  'tests/test_readlink.cc',
  'tests/test_rename.cc',
  'tests/test_setattr.cc',
  'tests/test_unlink.cc',
  'tests/test_write.cc',
  'tests/test_read.cc',
  'tests/test_xattr.cc',
)

# If your tests need the same deps as the filesystem client libraries, add `deps` too.
# If tests only exec ./fs and do syscalls, you can omit `deps`.
test_exe = executable(
  'fdbfs_tests',
  test_sources,
  dependencies : [catch2_dep],
  # Useful while developing integration tests:
  # cpp_args : cc.get_supported_arguments(['-O0', '-g']),
)

# Make ./fs available to the test at a stable path.
# Meson runs tests from the build dir, so you can reference it as:
#   fs_exe = join_paths(meson.current_build_dir(), 'fs')
#
# Pass it via an environment variable so your tests don't depend on CWD assumptions.
test(
  'fdbfs_integration_empty',
  test_exe,
  env : [
    'FDBFS_FS_EXE=' + fs_exe.full_path(),
    'FDBFS_SOURCE_DIR=' + meson.project_source_root(),
    'FDBFS_TEST_MATRIX=empty',
  ],
  timeout : 120,
  is_parallel : false,
)

test(
  'fdbfs_integration_seeded',
  test_exe,
  env : [
    'FDBFS_FS_EXE=' + fs_exe.full_path(),
    'FDBFS_SOURCE_DIR=' + meson.project_source_root(),
    'FDBFS_TEST_MATRIX=seeded',
  ],
  timeout : 300,
  is_parallel : false,
)

# Optional: a quick “smoke” test label so you can run subset:
# meson test --suite smoke
# test('fdbfs_smoke', test_exe, suite : ['smoke'], env : [...])
