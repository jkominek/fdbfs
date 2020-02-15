ifeq ($(origin CXX),default)
# this better be clang>=6.0
CXX=c++
endif
CXXFLAGS=-std=c++17 -fPIC -g -Wall `pkg-config fuse --cflags` `pkg-config liblz4 --cflags` `pkg-config libzstd --cflags` #-DDEBUG=1

fs: main.o inflight.o util.o values.pb.o getattr.o lookup.o readdir.o read.o open.o release.o mknod.o unlink.o link.o readlink.o symlink.o setattr.o rename.o write.o forget.o statfs.o garbage_collector.o getxattr.o setxattr.o removexattr.o
	$(CXX) $(CXXFLAGS) -pg -g -Wall $^  `pkg-config fuse --libs` `pkg-config --libs protobuf` `pkg-config --libs liblz4` `pkg-config --libs libzstd` -lfdb_c -lm -o fs

# close enough
values.pb.cc values.pb.h: values.proto
	protoc $< --cpp_out=.

%.o: %.cc inflight.h util.h values.pb.h fdbfs_ops.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	-rm -f fs *.o *~ values.pb.cc values.pb.h

values_pb2.py: values.proto
	protoc values.proto --python_out=.
regen: values_pb2.py
	./gen.py|fdbcli
