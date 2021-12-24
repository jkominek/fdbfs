#!/bin/sh

echo user_allow_other | sudo tee -a /etc/fuse.conf

rm -rf /tmp/fdb
mkdir /tmp/fdb
# we shouldn't see this after a mount
touch /tmp/fdb/XXX

# start fs
valgrind --log-file=/tmp/valgrind.txt build/fs -o default_permissions,allow_other,nonempty /tmp/fdb &

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
# let's look at the contents of the fdb filesystem
# we shouldn't see the XXX file.
ls -l
sudo prove -r /tmp/secfs.test/fstest/fstest

cd /tmp
umount /tmp/fdb
sleep 2
cat /tmp/valgrind.txt
