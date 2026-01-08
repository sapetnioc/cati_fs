/*
 * This file system mirrors the existing file system hierarchy of the
 * system, starting at the root file system. This is implemented by
 * just "passing through" all requests to the corresponding user-space
 * libc functions. This implementation is a little more sophisticated
 * than the one in passthrough.c, so performance is not quite as bad.
 *
 * Debian based system dependencies for compilation:
 * 
 *      sudo  apt-get install build-essential git meson wget pkg-config udev
 *      git clone https://github.com/libfuse/libfuse.git
 *      cd libfuse
 *      mkdir build
 *      cd build
 *      meson --buildtype release ..
 *      ninja
 *      sudo ninja install
 *      cd ../..
 *      rm -Rf libfuse
 * 
 * Compile with:
 *
 *      gcc -c -O3 sqlite3.c
 *      gcc -Wall -O3 cati_fs.c sqlite3.o `pkg-config fuse3 --cflags --libs` -ldl -Wl,-rpath=/usr/local/lib/x86_64-linux-gnu -o cati_fs
 * 
 */

#define FUSE_USE_VERSION 31

// #define DEBUG=1

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define _GNU_SOURCE

#include <fuse.h>

#ifdef HAVE_LIBULOCKMGR
#include <ulockmgr.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif
#include <sys/file.h> /* flock(2) */

#include <sqlite3.h>


static const char schema[] =
  "CREATE TABLE catifs(\n"
  "  path TEXT NOT NULL,\n"
  "  real_path TEXT,\n"
  "  st_dev INT,\n"
  "  st_ino INT PRIMARY KEY,\n"
  "  st_mode INT,\n"
  "  st_nlink INT,\n"
  "  st_uid INT,\n"
  "  st_gid INT,\n"
  "  st_rdev INT,\n"
  "  st_size INT,\n"
  "  st_blksize INT,\n"
  "  st_blocks INT,\n"
  "  st_atim_sec INT,\n"
  "  st_atim_nsec INT,\n"
  "  st_mtim_sec INT,\n"
  "  st_mtim_nsec INT,\n"
  "  st_ctim_sec INT,\n"
  "  st_ctim_nsec INT\n"
  ");\n"
  "CREATE UNIQUE INDEX idx_catifs_path ON catifs (path);\n"
  "CREATE TABLE catifs_attrs(\n"
  "  st_ino INT NOT NULL REFERENCES catifs (st_ino),\n"
  "  name TEXT NOT NULL,\n"
  "  value TEXT NOT NULL,\n"
  " PRIMARY KEY (st_ino, name)\n"
  ");";


static void *catifs_init(struct fuse_conn_info *conn,
		         struct fuse_config *cfg)
{
    (void) conn;

    /* Pick up changes from lower filesystem right away. This is
        also necessary for better hardlink support. When the kernel
        calls the unlink() handler, it does not know the inode of
        the to-be-removed entry and can therefore not invalidate
        the cache of the associated inode - resulting in an
        incorrect st_nlink value being reported for any remaining
        hardlinks to this inode. */
    cfg->entry_timeout = 0;
    cfg->attr_timeout = 0;
    cfg->negative_timeout = 0;

    return fuse_get_context()->private_data;
}

static void catifs_destroy(void *private_data)
{
#ifdef debug
    fpfrinf(stderr, "Closing database\n");
#endif
    sqlite3_close(private_data);
}

static int cati_getattr(const char *path, struct stat *buf,
			struct fuse_file_info *fi)
{
    (void) fi;
    int result;
    sqlite3_stmt *query = 0;
    int rc;
    sqlite3 *db;
    const char * sql;
    
    memset(buf, 0, sizeof(*buf));
    if( strcmp(path, "/")==0 ){
        buf->st_mode = S_IFDIR | 0755;
        buf->st_nlink = 2;
        return 0;
    }
    sql = "SELECT st_dev, st_ino, st_mode, st_nlink, st_uid, st_gid, "
            "st_rdev, st_size, st_blksize, st_blocks, st_atim_sec, "
            "st_atim_nsec, st_mtim_sec, st_mtim_nsec, st_ctim_sec, "
            "st_ctim_nsec FROM catifs WHERE path=?";
    db = fuse_get_context()->private_data;
    rc = sqlite3_prepare_v2(db,
            sql,
            -1, &query, 0);
    if( rc != SQLITE_OK ) {
#ifdef DEBUG
        fprintf(stderr, "getattr cannot prepare SQL query: %s\n", sqlite3_errmsg(db));
#endif
        return -ENOENT;
    }
    
    rc = sqlite3_bind_text(query, 1, path, -1, SQLITE_STATIC);
    if( rc != SQLITE_OK ) {
#ifdef DEBUG
        fprintf(stderr, "getattr cannot bind path in SQL query: %s\n", sqlite3_errmsg(db));
#endif
        return -ENOENT;
    }
    rc = sqlite3_step(query);
    if(  rc == SQLITE_ROW ) {
        buf->st_dev = sqlite3_column_int(query, 0);
        buf->st_ino = sqlite3_column_int(query, 1);
        buf->st_mode = sqlite3_column_int(query, 2);
        buf->st_nlink = sqlite3_column_int(query, 3);
        buf->st_uid = sqlite3_column_int(query, 4);
        buf->st_gid = sqlite3_column_int(query, 5);
        buf->st_rdev = sqlite3_column_int(query, 6);
        buf->st_size = sqlite3_column_int(query, 7);
        buf->st_blksize = sqlite3_column_int(query, 8);
        buf->st_blocks = sqlite3_column_int(query, 9);
        buf->st_atim.tv_sec = sqlite3_column_int(query, 10);
        buf->st_atim.tv_nsec = sqlite3_column_int(query, 11);
        buf->st_mtim.tv_sec = sqlite3_column_int(query, 12);
        buf->st_mtim.tv_nsec = sqlite3_column_int(query, 13);
        buf->st_ctim.tv_sec = sqlite3_column_int(query, 14);
        buf->st_ctim.tv_nsec = sqlite3_column_int(query, 15);
        result = 0;
    } else {
#ifdef DEBUG
        fprintf(stderr, "getattr SQL query failed: (%d) %s\n", rc, sqlite3_errmsg(db));
#endif
        result = -ENOENT;
    }
    rc = sqlite3_finalize(query);
#ifdef DEBUG
    if( rc != SQLITE_OK ) {
        fprintf(stderr, "getattr cannot finalize SQL query: %s\n", sqlite3_errmsg(db));
    }
#endif
    return result;
}

// static int catifs_access(const char *path, int mask)
// {
//     sqlite3 *db;
//     
//     db = fuse_get_context()->private_data;
//     
// 	int res;
// 
// 	res = access(path, mask);
// 	if (res == -1)
// 		return -errno;
// 
// 	return 0;
// }

static int add_path_to_database(sqlite3 *db, const char *from, const char *to)
{
    struct stat buf;
    int rc;
    sqlite3_stmt *query;
    int result;
    
    if (stat(from, &buf) == -1)
        return -errno;

    rc = sqlite3_prepare_v2(db,
            "INSERT INTO catifs (path, real_path, st_dev, st_mode, "
            "st_nlink, st_uid, st_gid, st_rdev, st_size, st_blksize, "
            "st_blocks, st_atim_sec, st_atim_nsec, st_mtim_sec, st_mtim_nsec, "
            "st_ctim_sec, st_ctim_nsec) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",
            -1, &query, 0);
    if( rc != SQLITE_OK ) {
#ifdef DEBUG
        fprintf(stderr, "symlink cannot prepare SQL query: %s\n", sqlite3_errmsg(db));
#endif
        return -EIO;
    }
    
    sqlite3_bind_text(query, 1, to, -1, SQLITE_STATIC);
    sqlite3_bind_text(query, 2, from, -1, SQLITE_STATIC);
    sqlite3_bind_int(query, 3, buf.st_dev);
    sqlite3_bind_int(query, 4, buf.st_mode);
    sqlite3_bind_int(query, 5, buf.st_nlink);
    sqlite3_bind_int(query, 6, buf.st_uid);
    sqlite3_bind_int(query, 7, buf.st_gid);
    sqlite3_bind_int(query, 8, buf.st_rdev);
    sqlite3_bind_int(query, 9, buf.st_size);
    sqlite3_bind_int(query, 10, buf.st_blksize);
    sqlite3_bind_int(query, 11, buf.st_blocks);
    sqlite3_bind_int(query, 12, buf.st_atim.tv_sec);
    sqlite3_bind_int(query, 13, buf.st_atim.tv_nsec);
    sqlite3_bind_int(query, 14, buf.st_mtim.tv_sec);
    sqlite3_bind_int(query, 15, buf.st_mtim.tv_nsec);
    sqlite3_bind_int(query, 16, buf.st_ctim.tv_sec);
    sqlite3_bind_int(query, 17, buf.st_ctim.tv_nsec);
    if ( sqlite3_step(query) == SQLITE_DONE)
        result = 0;
    else {
#ifdef DEBUG
        fprintf(stderr, "symlink insert item in database: %s\n", sqlite3_errmsg(db));
#endif
        result = -EIO;
    }
    sqlite3_finalize(query);
    return result;
}

// static int catifs_readlink(const char *path, char *buf, size_t size)
// {
//     sqlite3 *db;
//     int rc;
//     db = fuse_get_context()->private_data;
//     sqlite3_stmt *query;
//     int result;
//     
//     rc = sqlite3_prepare_v2(db,
//             "SELECT real_path FROM catifs WHERE path=?1",
//             -1, &query, 0);
//     if( rc != SQLITE_OK ) {
//         return -EIO;
//     }
//     
//     sqlite3_bind_text(query, 1, path, -1, SQLITE_STATIC);
//     if( sqlite3_step(query) == SQLITE_ROW ) {
//         strncpy(buf, (const char *) sqlite3_column_text(query, 0), size-1);
//         result = 0;
//     } else {
//         result = -ENOENT;
//     }
//     sqlite3_finalize(query);
//     return result;
// }


// static int catifs_opendir(const char *path, struct fuse_file_info *fi)
// {s
//     return 0;
// }

static inline struct xmp_dirp *get_dirp(struct fuse_file_info *fi)
{
	return (struct xmp_dirp *) (uintptr_t) fi->fh;
}

static int catifs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		          off_t offset, struct fuse_file_info *fi,
		          enum fuse_readdir_flags flags)
{
    (void) fi;
    sqlite3_stmt *query;
    int rc;
    sqlite3 *db;
    struct stat stbuf;
#ifdef DEBUG
    int count = 0;
#endif
    
    db = fuse_get_context()->private_data;
    rc = sqlite3_prepare_v2(db,
            "SELECT st_dev, st_ino, st_mode, st_nlink, st_uid, st_gid, "
            "st_rdev, st_size, st_blksize, st_blocks, st_atim_sec, "
            "st_atim_nsec, st_mtim_sec, st_mtim_nsec, st_ctim_sec, "
            "st_ctim_nsec, path FROM catifs WHERE "
            "path GLOB (?1 || '/*') AND instr(substr(path,length(?1)+2), '/') == 0",
            -1, &query, 0);
    if( rc != SQLITE_OK ) {
#ifdef DEBUG
        fprintf(stderr, "readdir cannot prepare SQL query: %s\n", sqlite3_errmsg(db));
#endif
        return -EIO;
    }

    if( strcmp(path, "/")==0 ){
        path = "";
    }
    rc = sqlite3_bind_text(query, 1, path, -1, SQLITE_STATIC);
    if( rc != SQLITE_OK ) {
#ifdef DEBUG
        fprintf(stderr, "readdir bind path to SQL query: %s\n", sqlite3_errmsg(db));
#endif
        sqlite3_finalize(query);
        return -EIO;
    }
    filler(buf, ".", NULL, 0, 0);
    filler(buf, "..", NULL, 0, 0);
#ifdef DEBUG
    char *sql = sqlite3_expanded_sql(query);
    fprintf(stderr, "readdir using SQL query: %s ; %s\n", path, sql);
    sqlite3_free(sql);
#endif
    const int dir_len = strlen(path)+1;
    for( rc = sqlite3_step(query); rc == SQLITE_ROW; rc = sqlite3_step(query) ) {
        stbuf.st_dev = sqlite3_column_int(query, 0);
        stbuf.st_ino = sqlite3_column_int(query, 1);
        stbuf.st_mode = sqlite3_column_int(query, 2);
        stbuf.st_nlink = sqlite3_column_int(query, 3);
        stbuf.st_uid = sqlite3_column_int(query, 4);
        stbuf.st_gid = sqlite3_column_int(query, 5);
        stbuf.st_rdev = sqlite3_column_int(query, 6);
        stbuf.st_size = sqlite3_column_int(query, 7);
        stbuf.st_blksize = sqlite3_column_int(query, 8);
        stbuf.st_blocks = sqlite3_column_int(query, 9);
        stbuf.st_atim.tv_sec = sqlite3_column_int(query, 10);
        stbuf.st_atim.tv_nsec = sqlite3_column_int(query, 11);
        stbuf.st_mtim.tv_sec = sqlite3_column_int(query, 12);
        stbuf.st_mtim.tv_nsec = sqlite3_column_int(query, 13);
        stbuf.st_ctim.tv_sec = sqlite3_column_int(query, 14);
        stbuf.st_ctim.tv_nsec = sqlite3_column_int(query, 15);
        filler(buf, ((const char *) sqlite3_column_text(query, 16)) + dir_len, &stbuf, 0, 0);
#ifdef DEBUG
        fprintf(stderr, "readdir ->: %s\n", ((const char *) sqlite3_column_text(query, 16)) + dir_len);
        ++count;
#endif
    }
    sqlite3_finalize(query);
    if ( rc != SQLITE_DONE ) {
#ifdef DEBUG
        fprintf(stderr, "readdir cannot query database: %s (%d)\n", sqlite3_errmsg(db), rc);
#endif
        return -EIO;
    } else {
#ifdef DEBUG
        fprintf(stderr, "readdir successful: %d entries\n", count);
#endif
        return 0;
    }
}

// static int catifs_releasedir(const char *path, struct fuse_file_info *fi)
// {
// }

// static int catifs_mknod(const char *path, mode_t mode, dev_t rdev)
// {
//     return 0;
// }

static int catifs_mkdir(const char *path, mode_t mode)
{
    char template[] = "/tmp/catifs.XXXXXX";
    sqlite3 *db;
    int result;
    int rc;
    sqlite3_stmt *query;
    
    db = fuse_get_context()->private_data;

    char *dir_name = mkdtemp(template);
    if ( ! dir_name ) {
#ifdef DEBUG
        fprintf(stderr, "mkdir cannot create temporary directory\n");
#endif
        return -errno;
    }
    add_path_to_database(db, dir_name, path);
    rmdir(dir_name);
    rc = sqlite3_prepare_v2(db,
            "UPDATE catifs SET real_path=NULL, st_mode=?2 WHERE path=?1",
//             "UPDATE catifs SET real_path=NULL WHERE path=?1",
            -1, &query, 0);
    
    if( rc != SQLITE_OK ) {
#ifdef DEBUG
        fprintf(stderr, "mkdir cannot prepare SQL query: %s\n", sqlite3_errmsg(db));
#endif
        return -EIO;
    }
    rc = sqlite3_bind_text(query, 1, path, -1, SQLITE_STATIC);
    if( rc != SQLITE_OK ) {
#ifdef DEBUG
        fprintf(stderr, "mkdir bind path to SQL query: %s\n", sqlite3_errmsg(db));
#endif
        sqlite3_finalize(query);
        return -EIO;
    }
    rc = sqlite3_bind_int(query, 2, mode|S_IFDIR);
    if( rc != SQLITE_OK ) {
#ifdef DEBUG
        fprintf(stderr, "mkdir bind mode to SQL query: %s\n", sqlite3_errmsg(db));
#endif
        sqlite3_finalize(query);
        return -EIO;
    }
    
    if ( sqlite3_step(query) == SQLITE_DONE)
        result = 0;
    else {
#ifdef DEBUG
        fprintf(stderr, "mkdir cannot modify database: %s\n", sqlite3_errmsg(db));
#endif
        result = -EIO;
    }
    
    rc = sqlite3_finalize(query);
    if ( rc != SQLITE_OK ) {
#ifdef DEBUG
        fprintf(stderr, "mkdir cannot delete query: %s\n", sqlite3_errmsg(db));
#endif
        return -EIO;
    }
    return result;
}

static int catifs_unlink(const char *path)
{
    sqlite3 *db;
    int rc;
    sqlite3_stmt *query;
    int result;
    
    db = fuse_get_context()->private_data;
    rc = sqlite3_prepare_v2(db,
            "DELETE FROM catifs WHERE path=?1",
            -1, &query, 0);
    if( rc != SQLITE_OK ) {
        return -EIO;
    }
    
    rc = sqlite3_bind_text(query, 1, path, -1, SQLITE_STATIC);
    if( rc != SQLITE_OK ) {
        return -EIO;
    }
    rc = sqlite3_step(query);
    if( rc == SQLITE_DONE ) {
        result = 0;
    } else {
#ifdef DEBUG
        fprintf(stderr, "unlink cannot remove path: %s\n", sqlite3_errmsg(db));
#endif
        result = -ENOENT;
    }
    sqlite3_finalize(query);
    return result;
}


static int catifs_rename(const char *from, const char *to, unsigned int flags)
{
    sqlite3 *db;
    int rc;
    sqlite3_stmt *query;
    int result;
    
    db = fuse_get_context()->private_data;
    rc = sqlite3_prepare_v2(db,
            "UPDATE catifs SET path = ?2 || substr(path, ?3) WHERE path=?1 OR path GLOB ?1 || '/*'",
            -1, &query, 0);
    if( rc != SQLITE_OK ) {
        return -EIO;
    }
    
    rc = sqlite3_bind_text(query, 1, from, -1, SQLITE_STATIC);
    if( rc != SQLITE_OK ) {
        return -EIO;
    }
    rc = sqlite3_bind_text(query, 2, to, -1, SQLITE_STATIC);
    if( rc != SQLITE_OK ) {
        return -EIO;
    }
    rc = sqlite3_bind_int(query, 3, strlen(from)+1);
    if( rc != SQLITE_OK ) {
        return -EIO;
    }
    rc = sqlite3_step(query);
    if( rc == SQLITE_DONE ) {
        result = 0;
    } else {
#ifdef DEBUG
        fprintf(stderr, "rename cannot rename path(s): %s\n", sqlite3_errmsg(db));
#endif
        result = -ENOENT;
    }
    sqlite3_finalize(query);
    return result;
}

// static int catifs_link(const char *from, const char *to)
// {
// }

static int catifs_chmod(const char *path, mode_t mode,
		        struct fuse_file_info *fi)
{
    sqlite3 *db;
    int rc;
    sqlite3_stmt *query;
    int result;
    
    db = fuse_get_context()->private_data;
    rc = sqlite3_prepare_v2(db,
            "UPDATE catifs SET st_mode=?2 WHERE path=?1",
            -1, &query, 0);
    if( rc != SQLITE_OK ) {
        return -EIO;
    }
    
    rc = sqlite3_bind_text(query, 1, path, -1, SQLITE_STATIC);
    if( rc != SQLITE_OK ) {
        return -EIO;
    }
    rc = sqlite3_bind_int(query, 2, mode);
    if( rc != SQLITE_OK ) {
        return -EIO;
    }
    rc = sqlite3_step(query);
    if( rc == SQLITE_DONE ) {
        result = 0;
    } else {
#ifdef DEBUG
        fprintf(stderr, "chmod SQL error: %s\n", sqlite3_errmsg(db));
#endif
        result = -ENOENT;
    }
    sqlite3_finalize(query);
    return result;
}

static int catifs_chown(const char *path, uid_t uid, gid_t gid,
		     struct fuse_file_info *fi)
{
    sqlite3 *db;
    int rc;
    sqlite3_stmt *query;
    int result;
    
    db = fuse_get_context()->private_data;
    if (uid == -1) {
        rc = sqlite3_prepare_v2(db,
                "UPDATE catifs SET st_gid=?3 WHERE path=?1",
                -1, &query, 0);
    } else if (gid == -1) {
        rc = sqlite3_prepare_v2(db,
                "UPDATE catifs SET st_uid=?2 WHERE path=?1",
                -1, &query, 0);
    } else {
        rc = sqlite3_prepare_v2(db,
                "UPDATE catifs SET st_uid=?2, st_gid=?3 WHERE path=?1",
                -1, &query, 0);
    }
    if( rc != SQLITE_OK ) {
        return -EIO;
    }
    
    rc = sqlite3_bind_text(query, 1, path, -1, SQLITE_STATIC);
    if( rc != SQLITE_OK ) {
        return -EIO;
    }
    if (uid != -1) {
        rc = sqlite3_bind_int(query, 2, uid);
        if( rc != SQLITE_OK ) {
            return -EIO;
        }
    }
    if (gid != -1) {
        rc = sqlite3_bind_int(query, 3, gid);
        if( rc != SQLITE_OK ) {
            return -EIO;
        }
    }
    rc = sqlite3_step(query);
    if( rc == SQLITE_DONE ) {
        result = 0;
    } else {
#ifdef DEBUG
        fprintf(stderr, "chmown SQL error: %s\n", sqlite3_errmsg(db));
#endif
        result = -ENOENT;
    }
    sqlite3_finalize(query);
    return result;
}

// static int catifs_truncate(const char *path, off_t size,
// 			 struct fuse_file_info *fi)
// {
// 	return 0;
// }


static int catifs_utimens(const char *path, const struct timespec ts[2],
		          struct fuse_file_info *fi)
{
    sqlite3 *db;
    int rc;
    sqlite3_stmt *query;
    int result;
    
    db = fuse_get_context()->private_data;
    if (ts[0].tv_nsec == UTIME_OMIT) {
        if (ts[1].tv_nsec == UTIME_OMIT) {
            return 0;
        }
        rc = sqlite3_prepare_v2(db,
                "UPDATE catifs SET st_mtim_sec=?4, st_mtim_nsec=?5 WHERE path=?1",
                -1, &query, 0);
    } else if (ts[1].tv_nsec == UTIME_OMIT) {
        rc = sqlite3_prepare_v2(db,
                "UPDATE catifs SET st_atim_sec=?2, st_atim_nsec=?3 WHERE path=?1",
                -1, &query, 0);
    } else {
        rc = sqlite3_prepare_v2(db,
                "UPDATE catifs SET st_atim_sec=?2, st_atim_nsec=?3, st_mtim_sec=?4, st_mtim_nsec=?5 WHERE path=?1",
                -1, &query, 0);
    }
    if( rc != SQLITE_OK ) {
        return -EIO;
    }
    
    rc = sqlite3_bind_text(query, 1, path, -1, SQLITE_STATIC);
    if( rc != SQLITE_OK ) {
        return -EIO;
    }
    if (ts[0].tv_nsec != UTIME_OMIT) {
        rc = sqlite3_bind_int(query, 2, ts[0].tv_sec);
        if( rc != SQLITE_OK ) {
            return -EIO;
        }
        rc = sqlite3_bind_int(query, 3, ts[0].tv_nsec);
        if( rc != SQLITE_OK ) {
            return -EIO;
        }
    }
    if (ts[1].tv_nsec != UTIME_OMIT) {
        rc = sqlite3_bind_int(query, 3, ts[1].tv_sec);
        if( rc != SQLITE_OK ) {
            return -EIO;
        }
        rc = sqlite3_bind_int(query, 4, ts[1].tv_nsec);
        if( rc != SQLITE_OK ) {
            return -EIO;
        }
    }
    rc = sqlite3_step(query);
    if( rc == SQLITE_DONE ) {
        result = 0;
    } else {
#ifdef DEBUG
        fprintf(stderr, "utimens SQL error: %s\n", sqlite3_errmsg(db));
#endif
        result = -ENOENT;
    }
    sqlite3_finalize(query);
    return result;
}


const char *real_path(const char *path)
{
    sqlite3 *db;
    const char * sql;
    int rc;
    sqlite3_stmt *query;
    const char *result = NULL;
    
    sql = "SELECT real_path FROM catifs WHERE path=?";
    db = fuse_get_context()->private_data;
    rc = sqlite3_prepare_v2(db,
            sql,
            -1, &query, 0);
    if( rc != SQLITE_OK ) {
#ifdef DEBUG
        fprintf(stderr, "getattr cannot prepare SQL query: %s\n", sqlite3_errmsg(db));
#endif
        return result;
    }
    
    rc = sqlite3_bind_text(query, 1, path, -1, SQLITE_STATIC);
    if( rc != SQLITE_OK ) {
#ifdef DEBUG
        fprintf(stderr, "getattr cannot bind path in SQL query: %s\n", sqlite3_errmsg(db));
#endif
        return result;
    }
    rc = sqlite3_step(query);
    if(  rc == SQLITE_ROW ) {
        result = strndup((const char *) sqlite3_column_text(query, 0), 4096);
    } else {
#ifdef DEBUG
        fprintf(stderr, "getattr SQL query failed: (%d) %s\n", rc, sqlite3_errmsg(db));
#endif
    }
    rc = sqlite3_finalize(query);
#ifdef DEBUG
    if( rc != SQLITE_OK ) {
        fprintf(stderr, "getattr cannot finalize SQL query: %s\n", sqlite3_errmsg(db));
    }
#endif
    return result;
}


static int catifs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    const char *rpath;
    int fd;

    rpath = real_path(path);
    fd = open(rpath, fi->flags, mode);
    if (fd == -1) return -errno;
    fi->fh = fd;
    return 0;
}

static int catifs_open(const char *path, struct fuse_file_info *fi)
{
    const char *rpath;
    int fd;

    rpath = real_path(path);
    fd = open(rpath, fi->flags);
    if (fd == -1) return -errno;
    fi->fh = fd;
    fprintf(stderr, "open %s = %s, %ld\n", path, rpath, fi->fh);
    return 0;
}

static int catifs_read(const char *path, char *buf, size_t size, off_t offset,
                       struct fuse_file_info *fi)
{
    int res;

    (void) path;
    res = pread(fi->fh, buf, size, offset);
    if (res == -1) res = -errno;
    return res;
}

static int catifs_read_buf(const char *path, struct fuse_bufvec **bufp,
                           size_t size, off_t offset, struct fuse_file_info *fi)
{
    struct fuse_bufvec *src;

    (void) path;

    src = malloc(sizeof(struct fuse_bufvec));
    if (src == NULL) return -ENOMEM;
    *src = FUSE_BUFVEC_INIT(size);

    src->buf[0].flags = FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK;
    src->buf[0].fd = fi->fh;
    src->buf[0].pos = offset;

    *bufp = src;

    return 0;
}

static int catifs_write(const char *path, const char *buf, size_t size,
                        off_t offset, struct fuse_file_info *fi)
{
    (void) path;
    int res;
    
    fprintf(stderr, "write %ld %ld %ld\n", fi->fh, offset, size);
    res = pwrite(fi->fh, buf, size, offset);
    if (res == -1) res = -errno;
    return res;
}

static int catifs_write_buf(const char *path, struct fuse_bufvec *buf,
                            off_t offset, struct fuse_file_info *fi)
{
    (void) path;
    struct fuse_bufvec dst = FUSE_BUFVEC_INIT(fuse_buf_size(buf));


    fprintf(stderr, "write buf\n");
    dst.buf[0].flags = FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK;
    dst.buf[0].fd = fi->fh;
    dst.buf[0].pos = offset;

    return fuse_buf_copy(&dst, buf, FUSE_BUF_SPLICE_NONBLOCK);
}

static int catifs_statfs(const char *path, struct statvfs *buf)
{
    const char *rpath;
    int res;

    rpath = real_path(path);
    res = statvfs(rpath, buf);
    if (res == -1) return -errno;
    return 0;
}

static int catifs_flush(const char *path, struct fuse_file_info *fi)
{
    int res;

    (void) path;
    /* This is called from every close on an open file, so call the
       close on the underlying filesystem.	But since flush may be
       called multiple times for an open file, this must not really
       close the file.  This is important if used on a network
       filesystem like NFS which flush the data/metadata on close() */
    res = close(dup(fi->fh));
    if (res == -1) return -errno;
    return 0;
}

static int catifs_release(const char *path, struct fuse_file_info *fi)
{
    (void) path;
    fprintf(stderr, "close %s %ld\n", path, fi->fh);
    close(fi->fh);
    return 0;
}

// static int catifs_fsync(const char *path, int isdatasync,
// 		     struct fuse_file_info *fi)
// {
// 	return 0;
// }

// #ifdef HAVE_POSIX_FALLOCATE
// static int catifs_fallocate(const char *path, int mode,
// 			off_t offset, off_t length, struct fuse_file_info *fi)
// {
// }
// #endif

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int xmp_setxattr(const char *path, const char *name, const char *value,
			size_t size, int flags)
{
	int res = lsetxattr(path, name, value, size, flags);
	if (res == -1)
		return -errno;
	return 0;
}

static int xmp_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
	int res = lgetxattr(path, name, value, size);
	if (res == -1)
		return -errno;
	return res;
}

static int xmp_listxattr(const char *path, char *list, size_t size)
{
	int res = llistxattr(path, list, size);
	if (res == -1)
		return -errno;
	return res;
}

static int xmp_removexattr(const char *path, const char *name)
{
	int res = lremovexattr(path, name);
	if (res == -1)
		return -errno;
	return 0;
}
#endif /* HAVE_SETXATTR */

// #ifdef HAVE_LIBULOCKMGR
// static int catifs_lock(const char *path, struct fuse_file_info *fi, int cmd,
// 		    struct flock *lock)
// {
// }
// #endif

// static int catifs_flock(const char *path, struct fuse_file_info *fi, int op)
// {
// 	return 0;
// }

static struct fuse_operations xmp_oper = {
    .init       = catifs_init,
    .destroy    = catifs_destroy,
    .getattr	= cati_getattr,
//    .access		= catifs_access,
//     .symlink	= catifs_symlink,
//     .readlink	= catifs_readlink,
//    .opendir	= catifs_opendir,
    .readdir    = catifs_readdir,
//     .releasedir	= catifs_releasedir,
//     .mknod		= catifs_mknod,
    .mkdir      = catifs_mkdir,
//     .unlink     = catifs_unlink,
    .rmdir	= catifs_unlink,
    .rename     = catifs_rename,
//     .link	= catifs_symlink,
    .chmod      = catifs_chmod,
    .chown      = catifs_chown,
//     .truncate   = catifs_truncate,
    .utimens    = catifs_utimens,
    .create     = catifs_create,
    .open       = catifs_open,
    .read       = catifs_read,
    .read_buf   = catifs_read_buf,
    .write      = catifs_write,
// Commented out until I find why writing to a file is not working
//     .write_buf  = catifs_write_buf,
    .statfs     = catifs_statfs,
    .flush      = catifs_flush,
    .release    = catifs_release,
//         .fsync      = catifs_fsync,
// #ifdef HAVE_POSIX_FALLOCATE
//     .fallocate	= catifs_fallocate,
// #endif
#ifdef HAVE_SETXATTR
// 	.setxattr	= xmp_setxattr,
// 	.getxattr	= xmp_getxattr,
// 	.listxattr	= xmp_listxattr,
// 	.removexattr	= xmp_removexattr,
#endif
// #ifdef HAVE_LIBULOCKMGR
// 	.lock		= catifs_lock,
// #endif
// 	.flock		= catifs_flock,
};


/*
 * Show a help message and quit.
 */
static void showHelp(const char *argv0) {
  fprintf(stderr, "Usage: %s [options] mount <database> <mount-point>\n", argv0);
  fprintf(stderr, "Usage: %s [options] add <database> <path> <dest_path>\n", argv0);
  fprintf(stderr,
     "Options:\n"
     "   -c      Create database if it does not exists\n"
  );
  exit(1);
}


static sqlite3 *open_database(const char *dbString, int createFlag) {
    sqlite3 *db;
    int flags;
    int rc;
    
    if (createFlag)
        flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI | SQLITE_OPEN_CREATE;
    else
        flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI;
    rc = sqlite3_open_v2(dbString, &db, flags, 0);
    if( rc != SQLITE_OK ) {
        fprintf(stderr, "Cannot open sqlite database: %s\n", dbString);
        exit(1);
    }
    rc = sqlite3_exec(db, "SELECT 1 FROM catifs LIMIT 1", 0, 0, 0);
    if( rc != SQLITE_OK && createFlag ){
        rc = sqlite3_exec(db, schema, 0, 0, 0);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Cannot create database schema: %s\n", sqlite3_errmsg(db));
            sqlite3_close(db);
            exit(1);
        }
        rc = sqlite3_exec(db, "SELECT 1 FROM catifs LIMIT 1", 0, 0, 0);
    }
    if( rc != SQLITE_OK ){
        fprintf(stderr, "Invalid SQLite database schema: %s\n", dbString);
        sqlite3_close(db);
        exit(1);
    }
    return db;
}

int main(int argc, char *argv[])
{
    umask(0);
    int i, j;
    int createFlag = 0;
    char *cmdString = 0;
    char *dbString = 0;
    char *mountPoint = 0;
    sqlite3 *db;
    char *fuseArgv[5];
    int result;
    
    for( i=1; i<argc; i++ ){
        if( argv[i][0] == '-' ) {
            for( j=1; argv[i][j]; j++ ) {
                switch( argv[i][j] ){
                    case 'c':
                        createFlag++;
                        break;
                    case '-':
                        break;
                    default:
                        showHelp(argv[0]);
                }
            }
        } else if ( cmdString == 0 ) {
            cmdString = argv[i];
        } else if ( dbString == 0 ) {
            dbString = argv[i];
        } else {
            break;
        }
    }
    if( cmdString == 0 || dbString==0 ) showHelp(argv[0]);
    if ( strcmp(cmdString, "mount" ) == 0) {
        if ( i == argc - 1 ) {
            mountPoint = argv[i];
            db = open_database(dbString, createFlag);
            fuseArgv[0] = argv[0];
            fuseArgv[1] = "-f"; // foreground
            fuseArgv[2] = "-s"; // single trheaded
#ifdef DEBUG
            fuseArgv[2] = "-d"; // single trheaded
            fprintf(stderr, "Database pointer: %p\n", db);
#endif
            fuseArgv[3] = mountPoint;
            fuseArgv[4] = 0;
            result = fuse_main(4, fuseArgv, &xmp_oper, db);
            return result;
        }
    } else if ( strcmp(cmdString, "add") == 0 ) {
        char *src;
        char *dst;
        
        if (i < argc) {
            src = argv[i++];
            if (i < argc) {
                dst = argv[i++];
                if (i == argc) {
                    db = open_database(dbString, createFlag);
                    return add_path_to_database(db, src, dst);
                }
            }
        }
    }
    showHelp(argv[0]);
}
