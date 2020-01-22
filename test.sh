#!/bin/bash
set -xe

mkdir -p test_a test_b
./fs test_a &
FUSEFS_A=$!
./fs test_b &
FUSEFS_B=$!
sleep 2

dd if=/dev/urandom of=/tmp/chunk bs=8192 count=10
dd if=/tmp/chunk of=test_a/chunk bs=37
cmp test_a/chunk test_b/chunk
cmp /tmp/chunk test_b/chunk

kill -1 $FUSEFS_A
kill -1 $FUSEFS_B

