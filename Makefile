fs: getattr.o  inflight.o  lookup.o  main.o  read.o  util.o readdir.o open.o
	gcc -Wall $^  `pkg-config fuse --libs` -lfdb_c -o fs

%.o: %.c inflight.h util.h
	gcc -Wall -c $< -o $@ `pkg-config fuse --cflags`

clean:
	-rm -f fs *.o

