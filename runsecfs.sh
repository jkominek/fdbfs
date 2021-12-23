#!/bin/sh

rm -rf /tmp/fdb
mkdir /tmp/fdb
valgrind --log-file=valgrind.txt ./fs -o default_permissions,allow_other /tmp/fdb &

echo sleeping for a bit, because of slowness
sleep 20
echo resuming...

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

umount /tmp/fdb
sleep 1
cat valgrind.txt
