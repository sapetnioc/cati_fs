from __future__ import print_function

from tempfile import mkdtemp
import os
import shutil
from subprocess import check_call, Popen
import time
import sys

osp= os.path


def tree(directory):
    for root, dirs, files in os.walk(directory):
        dirs = dict((i,tree(osp.join(root,i))) for i in dirs)
        return [dirs, files]

def main():
    expected = [{'test': 
                    [{'bidon': 
                        [{}, 
                        ['a_file']]},
                     []]},
                []]
    cati_fs = osp.realpath(osp.join(osp.dirname(sys.argv[0]), 'cati_fs'))
    tmp = mkdtemp(prefix='cati_fs_test')
    try:
        mountpoint = osp.join(tmp, 'cati_fs')
        os.mkdir(mountpoint)
        db = osp.join(tmp, 'cati_fs.sqlite')
        mount = Popen([cati_fs, 'mount', '-c', db, mountpoint])
        try:
            time.sleep(1)
            os.mkdir(tmp + '/bidon')
            with open(tmp + '/bidon/a_file', 'w') as f:
                f.write('something')
            if mount.poll() is None:
                os.mkdir(mountpoint + '/test')
                check_call([cati_fs, 'add', db, tmp + '/bidon', '/test/bidon'])            
                check_call([cati_fs, 'add', db, tmp + '/bidon/a_file', '/test/bidon/a_file'])            
                assert(tree(mountpoint) == expected)
                assert(open(mountpoint +'/test/bidon/a_file').read() == 'something')
                with open(mountpoint + '/test/bidon/a_file', 'w') as f:
                    f.write('something else')
                print(repr(open(mountpoint +'/test/bidon/a_file').read()))
                try:
                    assert(open(mountpoint +'/test/bidon/a_file').read() == 'something else')
                except AssertionError:
                    print(repr(open(mountpoint +'/test/bidon/a_file').read()))
                    raise
            else:
                print('ERROR: while mounting cati_fs', file=sys.stderr)
                return 1
        finally:
            check_call(['fusermount', '-u', mountpoint])
    finally:
        shutil.rmtree(tmp)
    return 0

if __name__ == '__main__':
    sys.exit(main())
