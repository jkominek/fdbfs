#!/bin/bash
set -xe

fdbcli --exec status

rm -rf test_a test_b
mkdir -p test_a test_b
build/fs test_a &
build/fs test_b &
sleep 2

df -h

touch test_a/quickcheck
touch test_b/quickcheck
ls -l test_a
ls -l test_b

dd if=/dev/urandom of=/tmp/chunk bs=8192 count=10
dd if=/tmp/chunk of=test_a/chunk bs=37
cmp test_a/chunk test_b/chunk
cmp /tmp/chunk test_b/chunk

umount test_a
umount test_b

sleep 2
