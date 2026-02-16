#!/bin/sh

echo user_allow_other | sudo tee -a /etc/fuse.conf

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

# reset fdbfs keyspace and recreate root inode/data fixture
if ! (cd "$SCRIPT_DIR" && ./gen.py | fdbcli); then
    echo failed to initialize FoundationDB contents via gen.py
    exit 1
fi

rm -rf /tmp/fdb
mkdir /tmp/fdb
# we shouldn't see this after a mount
touch /tmp/fdb/XXX

# start fs
valgrind --log-file=/tmp/valgrind.txt build/fs -o default_permissions,allow_other /tmp/fdb &

echo sleeping for a bit, because of slowness
sleep 20
df -h
echo resuming...
if [ -f /tmp/fdb/XXX ]; then
    echo failed to mount fdbfs; test is invalid
    exit 1
fi

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
actualexit=$?

cd /tmp
umount /tmp/fdb
sleep 2
cat /tmp/valgrind.txt

exit $actualexit
