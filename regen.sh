#!/bin/sh
protoc -Isrc --python_out=. values.proto
./gen.py | fdbcli
