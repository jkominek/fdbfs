CXX=clang++-6.0 -std=c++17 -fPIC

fs: main.o inflight.o util.o values.pb.o getattr.o lookup.o readdir.o read.o open.o mknod.o unlink.o link.o readlink.o symlink.o # write.o setattr.o rename.o
	$(CXX) -pg -g -Wall $^  `pkg-config fuse --libs` `pkg-config --libs protobuf` -lfdb_c -lm -o fs

values.pb.h: values.proto
	protoc values.proto --cpp_out=.

%.o: %.cc inflight.h util.h values.pb.h
	$(CXX) -DDEBUG=1 -g -Wall -c $< -o $@ `pkg-config fuse --cflags`

clean:
	-rm -f fs *.o *~

values_pb2.py: values.proto
	protoc values.proto --python_out=.
regen: values_pb2.py
	./gen.py|fdbcli
