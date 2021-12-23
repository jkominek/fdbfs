#!/bin/sh

mkdir /tmp/fdb
valgrind --log-file=valgrind.txt ./fs -o default_permissions,allow_other /tmp/fdb &
FUSEFS=$!


cd /tmp
git clone https://github.com/billziss-gh/secfs.test
cd /tmp/secfs.test/fstest/fstest
make
# cgofuse matches our current (intended) semantics
cat <<EOF > tests/conf
os=Linux
fs="cgofuse"
EOF
# remove xacl tests
rm -rf /tmp/secfs.test/fstest/fstest/tests/xacl

cd /tmp/fdb
sudo prove -r /tmp/secfs.test/fstest/fstest

kill -1 $FUSEFS
cat valgrind.txt
