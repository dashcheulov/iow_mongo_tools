from iowmongotools import fs


def test_localfsobserver_files(tmpdir):
    sfiles = list()
    subdir = tmpdir.join('subdir')
    subdir.mkdir()
    subsubdir = subdir.join('subdir')
    subsubdir.mkdir()
    for sdir, sfile in ((subdir, '1.tgz'), (tmpdir, '1.tgz'), (tmpdir, '2.log.gz'), (subdir, '3.log.gz'),
                        (tmpdir, '4.log.gz'), (subsubdir, '4.log'), (tmpdir, 'a12083480file_p1.tgz.1')):
        sfiles.append(sdir.join(sfile))
        sfiles[-1].write('v')
    abs_paths = lambda *x: set('/'.join((str(tmpdir.realpath()), str(f))) for f in x)
    observer = fs.LocalFilesObserver(fs.EventHandler(), {'path': tmpdir.realpath(), 'recursive': True})
    assert observer.files == abs_paths('subdir/1.tgz', '1.tgz', '2.log.gz', 'subdir/3.log.gz', '4.log.gz',
                                       'subdir/subdir/4.log', 'a12083480file_p1.tgz.1')
    observer = fs.LocalFilesObserver(fs.EventHandler(), {'path': tmpdir.realpath()})
    assert observer.files == abs_paths('1.tgz', '2.log.gz', '4.log.gz', 'a12083480file_p1.tgz.1')
    observer = fs.LocalFilesObserver(fs.EventHandler(),
                                     {'path': tmpdir.realpath(), 'filename': '.?\.log(.gz)?', 'recursive': True})
    assert observer.files == abs_paths('2.log.gz', 'subdir/3.log.gz', '4.log.gz', 'subdir/subdir/4.log')
    observer = fs.LocalFilesObserver(fs.EventHandler(), {'path': tmpdir.realpath(), 'filename': '.*\.tgz$'})
    assert observer.files == abs_paths('1.tgz')
