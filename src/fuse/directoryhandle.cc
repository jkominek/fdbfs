#include "directoryhandle.h"

DirectoryHandle::DirectoryHandle(fuse_ino_t ino) : ino_(ino) {}

void DirectoryHandle::close() { closed_ = true; }

bool DirectoryHandle::is_closed() const { return closed_; }

fuse_ino_t DirectoryHandle::inode() const { return ino_; }
