all: cati_fs

sqlite3.o: sqlite3.c
	gcc -c -O3 sqlite3.c

cati_fs: cati_fs.c sqlite3.o
	gcc -Wall -O3 cati_fs.c sqlite3.o `pkg-config fuse3 --cflags --libs` -ldl -Wl,-rpath=/usr/local/lib/x86_64-linux-gnu -o cati_fs
