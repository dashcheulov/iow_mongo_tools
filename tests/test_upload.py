from iowmongotools import upload
import pytest


def test_class_filedb_without_defined_path():
    with pytest.raises(NameError) as excinfo:
        upload.FileDB.load()
    assert str(excinfo.value) == 'Please define DB.path'


def test_class_filedb_loads_and_save_content(tmpdir):
    db_file = tmpdir.join('db.yaml')
    db_file.write('''
    property1: null
    property2: sample
    2.8: 3
    ''')
    upload.FileDB.load(db_file.realpath())
    assert upload.FileDB._FileDB__data == {'property1': None, 'property2': 'sample', 2.8: 3}
    upload.FileDB._FileDB__data.update({'property1': 'defined', 'property3': 'added'})
    del upload.FileDB._FileDB__data['property2']
    upload.FileDB._flushed = False
    upload.FileDB.flush()
    assert db_file.read_text('UTF-8') == 'property1: defined\n2.8: 3\nproperty3: added\n'
    del upload.FileDB._FileDB__data['property1']
    upload.FileDB.flush()
    assert db_file.read_text('UTF-8') == 'property1: defined\n2.8: 3\nproperty3: added\n'


def test_instance_filedb_update_get():
    db_without_init_val = upload.FileDB('test')
    db_with_init_val = upload.FileDB('test2', initial={'default': 'def_val'})
    db_without_init_val.update('item', 'value')
    db_with_init_val.update('item', 'value')
    assert db_without_init_val.get('default') is None
    assert db_without_init_val.get('item') == 'value'
    assert db_with_init_val.get('default') == 'def_val'
    assert db_with_init_val.get('item') == 'value'


def test_segmentfile_flags_set_get():
    segmentfile_instance = upload.SegmentFile('/fake_path', '')
    segmentfile_instance.flags.processed |= {'test'}
    segmentfile_instance.flags.invalid -= {'test'}
    assert segmentfile_instance.flags.invalid == frozenset()
    segmentfile_instance.flags.processed |= {'test1', 'test2'}
    assert segmentfile_instance.flags.processed == {'test', 'test1', 'test2'}
    segmentfile_instance.flags.processed -= {'test2'}
    segmentfile_instance.flags.invalid |= {'test'}
    upload.FileDB.load()
    segmentfile_instance.flags.invalid |= {'test2'}
    assert segmentfile_instance.flags.db.get('processed') == {'test', 'test1'}
    assert segmentfile_instance.flags.db.get('invalid') == {'test', 'test2'}


# Todo: add tests for SegmentFile, TmpSegmentFile, Uploader
