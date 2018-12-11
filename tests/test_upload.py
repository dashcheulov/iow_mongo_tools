from iowmongotools import upload
import pytest
import time
import os


def test_strategy_without_defined_update():
    with pytest.raises(AttributeError) as excinfo:
        upload.Strategy({'input': {}})


def test_strategy_prepare_template_params():
    sample_strategy_default = upload.Strategy({'input': {'text/csv': {}}, 'update': {'_id': ''}, 'collection': 'a.b'})
    sample_strategy = upload.Strategy(
        {'input': {'text/csv': {}}, 'templates': {'hash_of_segments': {'segment_separator': ':', 'retention': '1D1h'}},
         'update': {'_id': ''}, 'collection': 'a.b'})
    assert sample_strategy_default.template_params == {
        'hash_of_segments': {'retention': 2592000, 'segment_separator': ',',
                             'from_fields': ['segments']}}
    assert sample_strategy.template_params == {
        'hash_of_segments': {'retention': 90000, 'segment_separator': ':', 'from_fields': ['segments']}}


def test_strategy_get_hash_of_segments():
    sample_strategy = upload.Strategy({'input': {'text/csv': {}}, 'update': {'_id': ''}, 'collection': 'a.b'})
    sample_strategy2 = upload.Strategy(
        {'input': {'text/csv': {}}, 'templates': {'hash_of_segments': {'retention': '5D2m4s'}}, 'update': {'_id': ''},
         'collection': 'a.b'})
    expiration_ts = int(time.time() + 2592000)  # 30 days
    expiration_ts2 = int(time.time() + 432124)  # 5D2m4s
    assert sample_strategy._get_hash_of_segments(['678269,678272,765488,408098']) == {'678269': expiration_ts,
                                                                                      '678272': expiration_ts,
                                                                                      '765488': expiration_ts,
                                                                                      '408098': expiration_ts}
    assert sample_strategy2._get_hash_of_segments(['2341,2452_4234']) == {'2341': expiration_ts2,
                                                                          '2452_4234': expiration_ts2}


def test_strategy_timestamp():
    sample_strategy = upload.Strategy(
        {'input': {'text/csv': {}}, 'update': {'_id': '{{timestamp}}'}, 'collection': 'a.b'})
    current_ts = int(time.time())
    assert sample_strategy.get_setter('', {'titles': {}}) == {'_id': current_ts}


def test_strategy_parse_output():
    sample_strategy = upload.Strategy({'input': {'text/csv': {}}, 'update': {'_id': ''}, 'collection': 'a.b'})
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
    sample_strategy = upload.Strategy({'input': {'text/tab-separated-values': {
        'user_id': '^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}$',
        'bluekai_id': '.*',
        'campaign_ids': '.*',
        'segments': '^[0-9a-z_]+(?:,[0-9a-z_]+)*$'}},
        'update': {'_id': "{{user_id}}", 'dmp': {'bk': '{{hash_of_segments}}'}}, 'collection': 'a.b'})
    expiration_ts = int(time.time() + 2592000)  # 30 days
    assert sample_strategy.get_setter(
        ['cd59f2ca-5480-4fb9-b580-2e2f3194ce96', 'K68zJkWO99eQaG2q', '312041', '678269,678272,765488,408098'],
        sample_strategy.input['text/tab-separated-values']) == {
               '_id': 'cd59f2ca-5480-4fb9-b580-2e2f3194ce96',
               'dmp': {'bk': {'408098': expiration_ts,
                              '678269': expiration_ts,
                              '678272': expiration_ts,
                              '765488': expiration_ts}}}
    assert sample_strategy.get_setter(['b6dabebf-8e48-4465-a0dd-9a705b607255', '(UN', '$#D', '6782_s69,6'],
                                      sample_strategy.input['text/tab-separated-values']) == {
               '_id': 'b6dabebf-8e48-4465-a0dd-9a705b607255',
               'dmp': {'bk': {'6': expiration_ts, '6782_s69': expiration_ts}}}
    with pytest.raises(upload.BadLine):
        sample_strategy.get_setter(['b', 'e', '4', '6782_s69,6'], sample_strategy.input['text/tab-separated-values'])
    with pytest.raises(upload.BadLine):
        sample_strategy.get_setter(['b6dabebf-8e48-4465-a0dd-9a705b607255', 'e', '4', '67R82_s69,6'],
                                   sample_strategy.input['text/tab-separated-values'])


def test_segfile_counter():
    class MockResult(object):
        def __init__(self, matched_count, modified_count, upserted_count):
            self.matched_count = matched_count
            self.modified_count = modified_count
            self.upserted_count = upserted_count

    sample_counter = upload.SegmentFile.Counter()
    for result in ((1, 0, 1), (1, 0, 0), (0, 0, 1)):
        sample_counter.count_bulk_write_result(MockResult(*result))
    assert (sample_counter.matched, sample_counter.modified, sample_counter.upserted) == (2, 0, 2)
    assert str(sample_counter) == 'Lines: total - 0, invalid - 0. Documents: matched - 2, upserted - 2.'
    sample_counter.line_total += 1
    sample_counter2 = upload.SegmentFile.Counter()
    sample_counter2.line_total += 1
    for result in ((1, 2, 0), (0, 1, 0), (2, 0, 0)):
        sample_counter2.count_bulk_write_result(MockResult(*result))
    assert (sample_counter2.matched, sample_counter2.modified, sample_counter2.upserted) == (3, 3, 0)
    assert str(sample_counter2) == 'Lines: total - 1, invalid - 0. Documents: matched - 3, modified - 3.'
    assert str(sample_counter + sample_counter2) == \
           'Lines: total - 2, invalid - 0. Documents: matched - 5, modified - 3, upserted - 2.'


def test_fileemmiter_sorter(tmpdir):
    sfiles = dict()
    for sfile in ('s12083479file_p2.tgz', 's12083480file_p1.tgz', 'a12083480file_p1.log.gz', 'a12083479file_p3.log.gz',
                  's12083479file_p0.log.gz', 'a12083480file_p0.log.gz', 'a12083480file_p1.tgz'):
        sfiles[sfile] = tmpdir.join(sfile)
        sfiles[sfile].write('s')
        sfiles[sfile] = str(sfiles[sfile].realpath())
    sort1 = upload.FileEmitter.Sorter(
        {'file_path_regexp': '^.*/([a-z])([0-9]+).*p([0-9])\..*$',
         'order': ({'path.1': 'asc'}, {'path.2': 'asc'}, {'path.0': 'asc'}, {'stat.st_mtime': 'desc'})})
    assert list(map(os.path.basename, sort1.sort(sfiles.values()))) == ['s12083479file_p0.log.gz',
                                                                        's12083479file_p2.tgz',
                                                                        'a12083479file_p3.log.gz',
                                                                        'a12083480file_p0.log.gz',
                                                                        'a12083480file_p1.tgz',
                                                                        'a12083480file_p1.log.gz',
                                                                        's12083480file_p1.tgz']
    sort2 = upload.FileEmitter.Sorter(
        {'file_path_regexp': '^.*/([a-z])([0-9]+).*p([0-9])\..*$',
         'order': ({'path.2': 'desc'}, {'path.0': 'asc'}, {'stat.st_mtime': 'asc'})})
    assert list(map(os.path.basename, sort2.sort(sfiles.values()))) == ['a12083479file_p3.log.gz',
                                                                        's12083479file_p2.tgz',
                                                                        'a12083480file_p1.log.gz',
                                                                        'a12083480file_p1.tgz',
                                                                        's12083480file_p1.tgz',
                                                                        'a12083480file_p0.log.gz',
                                                                        's12083479file_p0.log.gz']
    sort3 = upload.FileEmitter.Sorter({'file_path_regexp': '^.*', 'order': ({'stat.st_ctime': 'desc'},)})
    assert list(map(os.path.basename, sort3.sort(sfiles.values()))) == ['a12083480file_p1.tgz',
                                                                        'a12083480file_p0.log.gz',
                                                                        's12083479file_p0.log.gz',
                                                                        'a12083479file_p3.log.gz',
                                                                        'a12083480file_p1.log.gz',
                                                                        's12083480file_p1.tgz',
                                                                        's12083479file_p2.tgz']
