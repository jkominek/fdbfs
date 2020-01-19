#!/bin/bash
set -xe

mkdir -p test
./fs test &
FUSEFS=$!
sleep 2

cd test
dd if=/dev/urandom of=/tmp/chunk bs=8192 count=10
dd if=/tmp/chunk of=chunk bs=13
cmp /tmp/chunk chunk

kill -1 $FUSEFS
