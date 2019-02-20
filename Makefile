fs: getattr.o  inflight.o  lookup.o  main.o  read.o  util.o readdir.o open.o mknod.o
	gcc -g -Wall $^  `pkg-config fuse --libs` -lfdb_c -o fs

%.o: %.c inflight.h util.h
	gcc -DDEBUG=1 -g -Wall -c $< -o $@ `pkg-config fuse --cflags`

clean:
	-rm -f fs *.o

