from pathlib import Path
import sqlite3
import stat
import sys


schema = """
CREATE TABLE catifs(
  path TEXT NOT NULL,
  real_path TEXT,
  st_dev INT,
  st_ino INT PRIMARY KEY,
  st_mode INT,
  st_nlink INT,
  st_uid INT,
  st_gid INT,
  st_rdev INT,
  st_size INT,
  st_blksize INT,
  st_blocks INT,
  st_atim_sec INT,
  st_atim_nsec INT,
  st_mtim_sec INT,
  st_mtim_nsec INT,
  st_ctim_sec INT,
  st_ctim_nsec INT,
  is_dir BOOL,
  is_link BOOL
);
CREATE UNIQUE INDEX idx_catifs_path ON catifs (path);
CREATE TABLE catifs_attrs(
  st_ino INT NOT NULL REFERENCES catifs (st_ino),
  name TEXT NOT NULL,
  value TEXT NOT NULL,
 PRIMARY KEY (st_ino, name)
);
"""


def size_to_string(fullSize):
    size = fullSize
    if size >= 1024:
        unit = "KiB"
        size /= 1024.0
        if size >= 1024:
            unit = "MiB"
            size /= 1024.0
            if size >= 1024:
                unit = "GiB"
                size /= 1024.0
                if size >= 1024:
                    unit = "TiB"
                    size /= 1024.0
        s = "%.2f" % (size,)
        if s.endswith(".00"):
            s = s[:-3]
        elif s[-1] == "0":
            s = s[:-1]
        return s + " " + unit + " (" + str(fullSize) + ")"
    else:
        return str(fullSize)


sqlite_file = sys.argv[1]
directory = Path(sys.argv[2])

create_schema = not Path(sqlite_file).exists()
with sqlite3.connect(sqlite_file) as database:
    if create_schema:
        database.executescript(schema)
    cursor = database.cursor()
    count = 0
    dir_count = 0
    link_count = 0
    file_count = 0
    size_count = 0
    for root, dirs, files in directory.walk():
        for name in dirs + files:
            real_path = root / name
            path = f"/{real_path.relative_to(directory)}"
            st = real_path.lstat()
            is_dir = stat.S_ISDIR(st.st_mode)
            is_link = stat.S_ISLNK(st.st_mode)

            cursor.execute(
                "INSERT INTO catifs (path, real_path, st_dev, st_mode, "
                "st_nlink, st_uid, st_gid, st_rdev, st_size, st_blksize, "
                "st_blocks, st_atim_sec, st_atim_nsec, st_mtim_sec, st_mtim_nsec, "
                "st_ctim_sec, st_ctim_nsec, is_dir, is_link) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",
                [
                    path,
                    str(real_path),
                    st.st_dev,
                    st.st_mode,
                    st.st_nlink,
                    st.st_uid,
                    st.st_gid,
                    st.st_rdev,
                    st.st_size,
                    st.st_blksize,
                    st.st_blocks,
                    st.st_atime,
                    st.st_atime_ns,
                    st.st_mtime,
                    st.st_mtime_ns,
                    st.st_ctime,
                    st.st_ctime_ns,
                    is_dir,
                    is_link,
                ],
            )
            count += 1
            if is_dir:
                dir_count += 1
            elif is_link:
                link_count += 1
            else:
                file_count += 1
                size_count += st.st_size
            if count % 1000 == 0:
                database.commit()
                print(
                    f"Stored {dir_count} directories, {file_count} files and {link_count} symlinks [{size_to_string(size_count)}]"
                )
                print(f"{count}: {path}")
