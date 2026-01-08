all: cati_fs

cati_fs: cati_fs.c
	gcc -Wall -O3 cati_fs.c `pkg-config sqlite3 --cflags --libs` `pkg-config fuse3 --cflags --libs` -ldl -Wl,-rpath=/usr/local/lib/x86_64-linux-gnu -o cati_fs
