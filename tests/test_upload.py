from iowmongotools import upload
import pytest
import time
import os
from copy import copy


def test_strategy_without_defined_update():
    with pytest.raises(AttributeError) as excinfo:
        upload.Strategy({'input': {}})


def test_strategy_timestamp():
    sample_strategy = upload.Strategy(
        {'input': {'text/csv': {}}, 'update_one': {'filter': {}, 'update': {'_id': '{{timestamp}}'}},
         'collection': 'a.b'})
    current_ts = int(time.time())
    assert sample_strategy.get_setter('', {'titles': {}}) == {'filter': {}, 'update': {'_id': current_ts}}


def test_strategy_parse_output():
    sample_strategy = upload.Strategy(
        {'input': {'text/csv': {}},
         'update_one': {'filter': {'_id': '{{uuid}}'}, 'update': {'dmp': '{{hash_of_segments}}'}}, 'collection': 'a.b'})
    assert sample_strategy._parse_output(
        {'_id': "{{user_id}}", 'dmp': {'bk': '{hash_of_segments}', 'fra': 'rg', 'some_key': '{{some_key}}'}},
        {'user_id': 'wefv', 'some_key': 'some_val'}) == {'_id': 'wefv', 'dmp': {'bk': '{hash_of_segments}', 'fra': 'rg',
                                                                                'some_key': 'some_val'}}
    expiration_ts = int(time.time() + 2592000)  # 30 days
    assert sample_strategy._parse_output({'_id': "{{user_id}}", 'dmp': {'bk': '{{hash_of_segments}}'}},
                                         {'user_id': 'wefv', 'segments': '678269,678272,765488,408098'}) == {
               '_id': 'wefv', 'dmp': {
            'bk': {'408098': expiration_ts, '678269': expiration_ts, '678272': expiration_ts, '765488': expiration_ts}}}
    with pytest.raises(upload.UnknownTemplate) as excinfo:
        sample_strategy._parse_output({'_id': "{{something_odd}}"}, {})
    assert str(excinfo.value) == 'Template \'something_odd\' is unknown.'


def test_strategy_get_setter():
    sample_strategy = upload.Strategy({'input': {'text/tab-separated-values': [{
        'user_id': '^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}$'},
        {'bluekai_id': '.*'},
        {'campaign_ids': '.*'},
        {'segments': '^[0-9a-z_]+(?:,[0-9a-z_]+)*$'}]},
        'update_one': {'filter': {'_id': "{{user_id}}"}, 'update': {'dmp': {'bk': '{{hash_of_segments}}'}}},
        'collection': 'a.b'})
    expiration_ts = int(time.time() + 2592000)  # 30 days
    assert sample_strategy.get_setter(
        ['cd59f2ca-5480-4fb9-b580-2e2f3194ce96', 'K68zJkWO99eQaG2q', '312041', '678269,678272,765488,408098'],
        sample_strategy.input['text/tab-separated-values']) == {
               'filter': {'_id': 'cd59f2ca-5480-4fb9-b580-2e2f3194ce96'},
               'update': {'dmp': {'bk': {'408098': expiration_ts,
                                         '678269': expiration_ts,
                                         '678272': expiration_ts,
                                         '765488': expiration_ts}}}}
    assert sample_strategy.get_setter(['b6dabebf-8e48-4465-a0dd-9a705b607255', '(UN', '$#D', '6782_s69,6'],
                                      sample_strategy.input['text/tab-separated-values']) == {
               'filter': {'_id': 'b6dabebf-8e48-4465-a0dd-9a705b607255'},
               'update': {'dmp': {'bk': {'6': expiration_ts, '6782_s69': expiration_ts}}}}
    with pytest.raises(upload.BadLine):
        sample_strategy.get_setter(['b', 'e', '4', '6782_s69,6'], sample_strategy.input['text/tab-separated-values'])
    with pytest.raises(upload.BadLine):
        sample_strategy.get_setter(['b6dabebf-8e48-4465-a0dd-9a705b607255', 'e', '4', '67R82_s69,6'],
                                   sample_strategy.input['text/tab-separated-values'])


def test_strategy_list_of_used_templates():
    sample_strategy = upload.Strategy(
        {'input': {'text/csv': [{}]},
         'update_one': {'filter': {'_id': "{{user_id}}"}, 'update': {'dmp': {'bk': '{{hash_of_segments}}'}}},
         'collection': 'a.b'})
    assert sample_strategy.set_of_used_templates(sample_strategy.output) == {'user_id', 'hash_of_segments'}
    assert sample_strategy.set_of_used_templates(
        {'filter': {'_id': "{{s}}"},
         'update': {'q': {'bk': '{{w}}', 'c': '{{g}}'}, 't': '{{g}}', 'v': {'w': '{{n}}'}}}) == {'s', 'w', 'g', 'n'}


def test_segfile_counter():
    class MockResult(object):
        def __init__(self, matched_count, modified_count, upserted_count):
            self.acknowledged = True
            self.matched_count = matched_count
            self.modified_count = modified_count
            self.upserted_count = upserted_count

    sample_counter = upload.SegfileCounter()
    for result in ((1, 0, 1), (1, 0, 0), (0, 0, 1)):
        sample_counter.count_bulk_write_result(MockResult(*result))
    assert (sample_counter.matched, sample_counter.modified, sample_counter.upserted) == (2, 0, 2)
    assert str(sample_counter) == 'Lines: total - 0, invalid - 0. Documents: matched - 2, upserted - 2.'
    sample_counter.line_total += 1
    sample_counter2 = upload.SegfileCounter()
    sample_counter2.line_total += 1
    for result in ((1, 2, 0), (0, 1, 0), (2, 0, 0)):
        sample_counter2.count_bulk_write_result(MockResult(*result))
    assert (sample_counter2.matched, sample_counter2.modified, sample_counter2.upserted) == (3, 3, 0)
    assert str(sample_counter2) == 'Lines: total - 1, invalid - 0. Documents: matched - 3, modified - 3.'
    assert str(sample_counter + sample_counter2) == \
           'Lines: total - 2, invalid - 0. Documents: matched - 5, modified - 3, upserted - 2.'
    assert str(sample_counter & sample_counter2) == \
           'Lines: total - 1, invalid - 0. Documents: matched - 5, modified - 3, upserted - 2.'


def test_fileemmiter_sorter(tmpdir):
    sfiles = dict()
    i = 0
    for sfile in ('s12083479file_p2.tgz', 's12083480file_p1.tgz', 'a12083480file_p1.log.gz', 'a12083479file_p3.log.gz',
                  's12083479file_p0.log.gz', 'a12083480file_p0.log.gz', 'a12083480file_p1.tgz'):
        i += 1
        sfiles[sfile] = tmpdir.join(sfile)
        sfiles[sfile].write('s' * i)
        sfiles[sfile] = str(sfiles[sfile].realpath())
    sort1 = upload.FileEmitter.Sorter(
        {'file_path_regexp': '^.*/([a-z])([0-9]+).*p([0-9])\..*$',
         'order': ({'path.1': 'asc'}, {'path.2': 'asc'}, {'path.0': 'asc'}, {'stat.st_size': 'desc'})})
    assert list(map(os.path.basename, sort1.sort(sfiles.values()))) == ['s12083479file_p0.log.gz',
                                                                        's12083479file_p2.tgz',
                                                                        'a12083479file_p3.log.gz',
                                                                        'a12083480file_p0.log.gz',
                                                                        'a12083480file_p1.tgz',
                                                                        'a12083480file_p1.log.gz',
                                                                        's12083480file_p1.tgz']
    sort2 = upload.FileEmitter.Sorter(
        {'file_path_regexp': '^.*/([a-z])([0-9]+).*p([0-9])\..*$',
         'order': ({'path.2': 'desc'}, {'path.0': 'asc'}, {'stat.st_size': 'asc'})})
    assert list(map(os.path.basename, sort2.sort(sfiles.values()))) == ['a12083479file_p3.log.gz',
                                                                        's12083479file_p2.tgz',
                                                                        'a12083480file_p1.log.gz',
                                                                        'a12083480file_p1.tgz',
                                                                        's12083480file_p1.tgz',
                                                                        'a12083480file_p0.log.gz',
                                                                        's12083479file_p0.log.gz']
    sort3 = upload.FileEmitter.Sorter({'file_path_regexp': '^.*', 'order': ({'stat.st_size': 'desc'},)})
    assert list(map(os.path.basename, sort3.sort(sfiles.values()))) == ['a12083480file_p1.tgz',
                                                                        'a12083480file_p0.log.gz',
                                                                        's12083479file_p0.log.gz',
                                                                        'a12083479file_p3.log.gz',
                                                                        'a12083480file_p1.log.gz',
                                                                        's12083480file_p1.tgz',
                                                                        's12083479file_p2.tgz']
    sort4 = upload.FileEmitter.Sorter({'file_path_regexp': '^Liveramp.*', 'order': ({'stat.st_size': 'desc'},)})
    with pytest.raises(upload.InvalidSegmentFile):
        list(map(os.path.basename, sort4.sort(sfiles.values())))


def test_counter():
    sample_counter = upload.Counter()
    for filename in ['file{}'.format(i) for i in range(10)]:
        for cluster in range(4):
            segfilecnt = upload.SegfileCounter()
            segfilecnt.matched = 1
            segfilecnt.upserted = 1
            segfilecnt.line_total = 10
            segfilecnt.line_invalid = 5
            sample_counter.count_result((filename, 1, segfilecnt, 'liveramp', str(cluster)))
    assert str(
        sample_counter) == 'Total files: processed - 10, invalid - 10. Lines: total - 100, invalid - 50. Documents: matched - 40, upserted - 40.'
    assert str(sample_counter._aggregate_counters(
        clusters=('2', '0'))) == 'Lines: total - 100, invalid - 50. Documents: matched - 20, upserted - 20.'
    sample_counter.__init__()
    sample_counter = upload.Counter()
    for filename in ['file{}'.format(i) for i in range(5)]:
        for cluster in range(3):
            segfilecnt = upload.SegfileCounter()
            segfilecnt.matched = 0
            segfilecnt.modified = 2
            segfilecnt.line_total = 4
            segfilecnt.line_invalid = 1
            sample_counter.count_result((filename, 0, segfilecnt, filename, str(cluster)))
    assert str(
        sample_counter) == 'Total files: processed - 5. Lines: total - 20, invalid - 5. Documents: matched - 0, modified - 30.'
    assert str(sample_counter._aggregate_counters(
        providers=('file0', 'file4'))) == 'Lines: total - 8, invalid - 2. Documents: matched - 0, modified - 12.'
    assert str(sample_counter._aggregate_counters(
        ('file2', 'file1'), ('1', '2'))) == 'Lines: total - 8, invalid - 2. Documents: matched - 0, modified - 8.'
    sample_counter.__init__()
    for filename in ['file{}'.format(i) for i in range(5)]:
        for cluster in range(5):
            sample_counter.count_result((filename, 0, None, 'provider', str(cluster)))
    assert str(sample_counter) == 'Total files: processed - 0, skipped - 5. '
    sample_counter.__init__()
    for filename in ['file{}'.format(i) for i in range(5)]:
        sample_counter.count_result((filename, 0, None, 'provider', 's'))
    for cluster in range(2):
        segfilecnt = upload.SegfileCounter()
        segfilecnt.matched = 5
        segfilecnt.modified = 5
        segfilecnt.upserted = 1
        segfilecnt.line_total = 5
        segfilecnt.line_invalid = 0
        sample_counter.count_result(('file1', 0, segfilecnt, 'liveramp', str(cluster)))
        sample_counter.count_result(('file10', 0, copy(segfilecnt), 'liveramp', str(cluster)))
    assert str(
        sample_counter) == 'Total files: processed - 2, skipped - 4. Lines: total - 10, invalid - 0. Documents: matched - 20, modified - 20, upserted - 4.'


def test_segment_file_tsv(tmpdir):
    with pytest.raises(TypeError):
        upload.SegmentFile('liveramp', 'unexistent_file', 'not_strategy')
    tsv_file = tmpdir.join('tsv_file.tsv')
    tsv_file.write(
        '''f35ac18d-de62-42d1-97b5-ac6136187451\t1995228346\n0100e0ba-5c29-4d2c-8a23-0c2e76bc38df\t1000812376''')
    sample_strategy = upload.Strategy({'input': {'text/tab-separated-values': [{
        'user_id': '^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}$'},
        {'segments': '^[0-9a-z_]+(?:,[0-9a-z_]+)*$'}]},
        'update_one': {'filter': {'_id': "{{user_id}}"}, 'update': {'$set': {'lvmp': '{{segments}}'}}},
        'collection': 'a.b'})
    with pytest.raises(FileNotFoundError):
        upload.SegmentFile('liveramp', 'unexistent_file', sample_strategy)
    segfile = upload.SegmentFile(str(tsv_file.realpath()), 'liveramp', sample_strategy)
    assert list(segfile.get_line()) == ['f35ac18d-de62-42d1-97b5-ac6136187451\t1995228346',
                                        '0100e0ba-5c29-4d2c-8a23-0c2e76bc38df\t1000812376']
    assert segfile.get_setter(next(segfile.get_line())) == [upload.UpdateOne(
        {'_id': 'f35ac18d-de62-42d1-97b5-ac6136187451'},
        {'$set': {'lvmp': '1995228346'}})]
    assert next(segfile.get_batch()) == [
        upload.UpdateOne({'_id': 'f35ac18d-de62-42d1-97b5-ac6136187451'}, {'$set': {'lvmp': '1995228346'}}, False),
        upload.UpdateOne({'_id': '0100e0ba-5c29-4d2c-8a23-0c2e76bc38df'}, {'$set': {'lvmp': '1000812376'}}, False)]
    with pytest.raises(upload.InvalidSegmentFile):
        segfile.load_metadata({'provider': 'lotame', 'invalid': True,
                               'processed': True,
                               'timer': {'started_ts': 1545820888.727645, 'finished_ts': 1545821147.86029},
                               'counter': {'matched': 0, 'modified': 0, 'upserted': 0, 'line_cur': 3455803,
                                           'line_invalid': 1267, 'line_total': 3455803}})
    segfile.load_metadata({'_id': 'audiencemembership_2018122500',
                           'path': '/Users/denis/iponweb/clearstream/lotame/2018122500/4827/audiencemembership.tsv.gz',
                           'provider': 'liveramp', 'type': ['text/tab-separated-values', 'gzip'], 'invalid': True,
                           'processed': True,
                           'timer': {'started_ts': 1545820888.727645, 'finished_ts': 1545821147.86029},
                           'counter': {'matched': 0, 'modified': 0, 'upserted': 0, 'line_cur': 3455803,
                                       'line_invalid': 1267, 'line_total': 3455803}})
    assert segfile.dump_metadata() == {'_id': 'tsv_file',
                                       'counter': {'line_cur': 3455803, 'line_invalid': 1267, 'line_total': 0,
                                                   'matched': 0, 'modified': 0, 'upserted': 0}, 'invalid': True,
                                       'path': tsv_file.realpath(),
                                       'processed': True, 'provider': 'liveramp',
                                       'timer': {'finished_ts': 1545821147.86029, 'started_ts': 1545820888.727645},
                                       'type': ('text/tab-separated-values', None)}


def test_timer():
    timer = upload.Timer()
    start_ts = time.time()
    timer.start()
    time.sleep(0.0000001)
    finish_ts = time.time()
    timer.stop()
    assert int(timer.started_ts) == int(start_ts)
    assert int(timer.finished_ts) == int(finish_ts)
    assert str(timer) == 'Processing time - 0 hours 0 minutes 0 seconds.'


def test_file_emitter(tmpdir):
    tsv_file = tmpdir.join('tsv_file.tsv')
    tsv_file.write('s')
    csv_file = tmpdir.join('csv_file.csv')
    csv_file.write('s')
    file_emitter = upload.FileEmitter('liveramp', {'delivery': {'local': {'path': '/tmp'}}, 'input': {'text/csv': {}},
                                                   'update_one': {'filter': {'_id': '{{uuid}}'},
                                                                  'update': {'dmp': '{{hash_of_segments}}'}},
                                                   'collection': 'a.b'})
    file_emitter.on_file_discovered(str(tsv_file.realpath()))
    assert file_emitter.errors.is_set()
    file_emitter.errors.clear()
    file_emitter.on_file_discovered(str(csv_file.realpath()))
    assert not file_emitter.errors.is_set()
    assert file_emitter.items_ready.is_set()
    assert isinstance(file_emitter.queue.get(), upload.SegmentFile)
