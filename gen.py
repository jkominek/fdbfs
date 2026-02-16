#!/usr/bin/env python3

# this spits out commands for fdbcli so you don't have to install
# the python module, and because it can get away with it.

import values_pb2 as msgs
import struct
import zstd
import string
#import lz4.block
printable_bytes = b"""0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~"""

import sys
prefix = b"FS"
if len(sys.argv)>1:
    prefix = sys.argv[1].encode('ascii')

print("writemode on")
print("begin")
print("clearrange {} {}\\xFF".format(prefix.decode('ascii'),
                                     prefix.decode('ascii')))

def inode_key(i):
    return prefix + struct.pack(">cQ", b"i", i)
def dirent_key(i, n):
    return prefix + struct.pack(">cQ", b"d", i) + n
def block_key(i, b):
    return prefix + struct.pack(">cQQ", b"f", i, b)
def xattr_node_key(i, n=''):
    return prefix + struct.pack(">cQ", b"x", i) + n
def xattr_data_key(i, n=''):
    return prefix + struct.pack(">cQ", b"X", i) + n
def printable(s):
    return "".join([bytes([c]).decode('ascii') if c in printable_bytes else ("\\x%02x" % (c,)) for c in s])
def print_set(k, v):
    if len(v)==0:
        print("set {} ''".format(printable(k)))
    else:
        print("set {} {}".format(printable(k), printable(v)))

rootdir_inode = 1

rootdir_inode_value = msgs.INodeRecord()
rootdir_inode_value.inode = rootdir_inode
rootdir_inode_value.size = 0
rootdir_inode_value.type = msgs.ft_directory
rootdir_inode_value.nlinks = 2
rootdir_inode_value.mode = 0o555

print_set(inode_key(rootdir_inode), rootdir_inode_value.SerializeToString())

hello_inode = 42
hello_inode_data = b"hello world"

hello_inode_value = msgs.INodeRecord()
hello_inode_value.inode = hello_inode
hello_inode_value.size = len(hello_inode_data)
hello_inode_value.type = msgs.ft_regular
hello_inode_value.nlinks = 1
hello_inode_value.mode = 0o666

hello_xattr_node = msgs.XAttrRecord()
print_set(xattr_node_key(hello_inode, b"greeting"),
          hello_xattr_node.SerializeToString())
print_set(xattr_data_key(hello_inode, b"greeting"),
          b"bonjour!")

hello_dirent = msgs.DirectoryEntry()
hello_dirent.inode = hello_inode
hello_dirent.type = msgs.ft_regular
print_set(dirent_key(rootdir_inode, b"hello"), hello_dirent.SerializeToString())
print_set(inode_key(hello_inode), hello_inode_value.SerializeToString())
print_set(block_key(hello_inode, 0), hello_inode_data)

world_inode = 666
world_inode_data = b"hello world!!!!"

world_inode_value = msgs.INodeRecord()
world_inode_value.inode = world_inode
world_inode_value.size = len(world_inode_data)
world_inode_value.type = msgs.ft_regular
world_inode_value.nlinks = 1
world_inode_value.mode = 0o666

world_dirent = msgs.DirectoryEntry()
world_dirent.inode = world_inode
world_dirent.type = msgs.ft_regular

print_set(dirent_key(rootdir_inode, b"world"), world_dirent.SerializeToString())
print_set(inode_key(world_inode), world_inode_value.SerializeToString())
print_set(block_key(world_inode, 0)+b"z\x01\x01", zstd.ZSTD_compress(world_inode_data, 22))


portal_inode = 100
portal_inode_value = msgs.INodeRecord()
portal_inode_value.inode = portal_inode
portal_inode_value.type = msgs.ft_symlink
portal_inode_value.nlinks = 1
portal_inode_value.mode = 0
portal_inode_value.symlink = b"world"

portal_dirent = msgs.DirectoryEntry()
portal_dirent.inode = portal_inode
portal_dirent.type = msgs.ft_symlink

print_set(dirent_key(rootdir_inode, b"portal"), portal_dirent.SerializeToString())
print_set(inode_key(portal_inode), portal_inode_value.SerializeToString())

print("commit")
