project('fdbfs', 'cpp',
        default_options : ['cpp_std=c++17'])

protoc = find_program('protoc', required : true)

deps = [
    dependency('fuse'),
    dependency('libzstd'),
    dependency('protobuf'),
    dependency('foundationdb-client')
]

project_source_files = [ ]
generated = [ ]

subdir('src')

executable('fs',
           project_source_files,
           generated,
           dependencies: deps
           )
