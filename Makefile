fs: getattr.o  inflight.o  lookup.o  main.o  read.o  util.o readdir.o open.o mknod.o unlink.o link.o values.pb-c.o readlink.o
	gcc -pg -g -Wall $^  `pkg-config fuse --libs` `pkg-config --libs 'libprotobuf-c >= 1.0.0'` -lfdb_c -lm -o fs

values.pb-c.h: values.proto
	protoc-c values.proto --c_out=.

%.o: %.c inflight.h util.h values.pb-c.h
	gcc -DDEBUG=1 -g -Wall -c $< -o $@ `pkg-config fuse --cflags`

clean:
	-rm -f fs *.o *~

values_pb2.py: values.proto
	protoc values.proto --python_out=.
