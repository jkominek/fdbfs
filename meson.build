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
  'fdbfs_integration',
  test_exe,
  env : [
    'FDBFS_FS_EXE=' + fs_exe.full_path(),
    'FDBFS_SOURCE_DIR=' + meson.project_source_root(),
  ],
  # Integration tests may be slower / occasionally need more time.
  timeout : 120,
  # Optional: stop the test run after the first failing test case (Catch2 supports --abortx).
  # args : ['--abortx', '1'],
)

# Optional: a quick “smoke” test label so you can run subset:
# meson test --suite smoke
# test('fdbfs_smoke', test_exe, suite : ['smoke'], env : [...])
