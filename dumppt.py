#!/usr/bin/env python3

# This prints out the process table for the filesysem

import struct
import fdb
import values_pb2 as msgs

fdb.api_version(620)
db = fdb.open()

prefix = b'FSp'

for i in db[prefix:prefix+b'\xff']:
    k = i.key[len(prefix):]

    pte = msgs.ProcessTableEntry()
    pte.ParseFromString(i.value)
    print(k, pte)
