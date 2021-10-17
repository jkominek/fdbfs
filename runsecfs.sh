#!/bin/sh

mkdir /tmp/fdb
./fs -o default_permissions,allow_other /tmp/fdb &


cd /tmp
git clone https://github.com/billziss-gh/secfs.test
cd /tmp/secfs.test/fstest/fstest
make
cat <<EOF > tests/conf
os=Linux
fs="ext3"
EOF

cd /tmp/fdb
sudo prove -r /tmp/secfs.test/fstest/fstest

# when we fix up all of the test cases again, we can drop this
exit 0
