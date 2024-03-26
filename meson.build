project('fdbfs', 'cpp',
        default_options : ['cpp_std=c++17'
                          ]
       )
cc = meson.get_compiler('cpp')
add_project_arguments(
  cc.get_supported_arguments([
    '-Wall'
  ]),
  language : 'cpp'
)

protoc = find_program('protoc', required : true)

deps = [
    dependency('fuse3'),
    dependency('libzstd'),
    dependency('protobuf'),
    dependency('foundationdb-client'),
    dependency('nlohmann_json')
]

project_source_files = [ ]
generated = [ ]

subdir('src')

executable('fs',
           project_source_files,
           generated,
           dependencies: deps
           )
