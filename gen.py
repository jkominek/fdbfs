#!/usr/bin/env python

import values_pb2 as msgs
import struct

prefix = "FS"

print "writemode on"
print "begin"
print "clearrange {} {}\\xFF".format(prefix, prefix)

def inode_key(i):
    return prefix + struct.pack(">cQ", "i", i)
def dirent_key(i, n):
    return prefix + struct.pack(">cQc", "i", i, "d") + n
def block_key(i, b):
    return prefix + struct.pack(">cQcQ", "i", i, "f", b)
def printable(s):
    return "".join([c if c.isalnum() else ("\\x%02x" % (ord(c),)) for c in s])
def print_set(k, v):
    print "set {} {}".format(printable(k), printable(v))

rootdir_inode = 1

rootdir_inode_value = msgs.INodeRecord()
rootdir_inode_value.inode = rootdir_inode
rootdir_inode_value.size = 0
rootdir_inode_value.type = msgs.directory
rootdir_inode_value.nlinks = 2
rootdir_inode_value.mode = 0555

print_set(inode_key(rootdir_inode), rootdir_inode_value.SerializeToString())

hello_inode = 42
hello_inode_data = "hello world"

hello_inode_value = msgs.INodeRecord()
hello_inode_value.inode = hello_inode
hello_inode_value.size = len(hello_inode_data)
hello_inode_value.type = msgs.regular
hello_inode_value.nlinks = 1
hello_inode_value.mode = 0666

hello_dirent = msgs.DirectoryEntry()
hello_dirent.inode = hello_inode
hello_dirent.type = msgs.regular
print_set(dirent_key(rootdir_inode, "hello"), hello_dirent.SerializeToString())
print_set(inode_key(hello_inode), hello_inode_value.SerializeToString())
print_set(block_key(hello_inode, 0), hello_inode_data)

world_inode = 666
world_inode_data = "hello world!!!!"

world_inode_value = msgs.INodeRecord()
world_inode_value.inode = world_inode
world_inode_value.size = len(world_inode_data)
world_inode_value.type = msgs.regular
world_inode_value.nlinks = 1
world_inode_value.mode = 0666

world_dirent = msgs.DirectoryEntry()
world_dirent.inode = world_inode
world_dirent.type = msgs.regular

print_set(dirent_key(rootdir_inode, "world"), world_dirent.SerializeToString())
print_set(inode_key(world_inode), world_inode_value.SerializeToString())
print_set(block_key(world_inode, 0), world_inode_data)

print "commit"
