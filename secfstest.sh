#!/bin/bash
set -xe

git clone --depth 1 --single-branch https://github.com/billziss-gh/secfs.test.git
cd secfs.test/fstest/fstest/
make
mkdir x
# TODO i'm missing something regarding the permissions, or running
# the tests, or something. i didn't have so many failing before.
../../../fs -o default_permissions,allow_root,allow_other x &
cd x
prove -r ../tests
