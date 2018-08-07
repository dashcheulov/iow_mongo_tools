from iowmongotools import upload
import pytest


def test_class_db_without_defined_path():
    with pytest.raises(NameError) as excinfo:
        upload.DB.load()
    assert str(excinfo.value) == 'Please define DB.path'


def test_class_db_loads_and_save_content(tmpdir):
    db_file = tmpdir.join('db.yaml')
    db_file.write('''
    property1: null
    property2: sample
    2.8: 3
    ''')
    upload.DB.path = db_file.realpath()
    upload.DB.load()
    assert upload.DB._DB__data == {'property1': None, 'property2': 'sample', 2.8: 3}
    upload.DB._DB__data.update({'property1': 'defined', 'property3': 'added'})
    del upload.DB._DB__data['property2']
    upload.DB.flush()
    assert db_file.read_text('UTF-8') == 'property1: defined\n2.8: 3\nproperty3: added\n'


def test_instance_db_update_get(tmpdir):
    db_without_init_val = upload.DB('test')
    db_with_init_val = upload.DB('test', initial={'default': 'def_val'})
    db_without_init_val.update({'item': 'value'})
    db_with_init_val.update({'item': 'value'})
    assert db_without_init_val.get('default') is None
    assert db_without_init_val.get('item') == 'value'
    assert db_with_init_val.get('default') == 'def_val'
    assert db_with_init_val.get('item') == 'value'

# Todo: add tests for SegmentFile, TmpSegmentFile, Uploader
