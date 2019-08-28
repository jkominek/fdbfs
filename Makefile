CXX=clang++-6.0 -std=c++17 -fPIC -g -Wall `pkg-config fuse --cflags` -DDEBUG=1 -DINDUCE_FAILURES=1

fs: main.o inflight.o util.o values.pb.o getattr.o lookup.o readdir.o read.o open.o mknod.o unlink.o link.o readlink.o symlink.o setattr.o rename.o write.o
	$(CXX) -pg -g -Wall $^  `pkg-config fuse --libs` `pkg-config --libs protobuf` -lfdb_c -lm -o fs

# close enough
values.pb.cc values.pb.h: values.proto
	protoc $< --cpp_out=.

%.o: %.cc inflight.h util.h values.pb.h
	$(CXX) -c $< -o $@

clean:
	-rm -f fs *.o *~ values.pb.cc values.pb.h

values_pb2.py: values.proto
	protoc values.proto --python_out=.
regen: values_pb2.py
	./gen.py|fdbcli
