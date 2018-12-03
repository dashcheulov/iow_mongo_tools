from iowmongotools import upload
import pytest
import time


def test_strategy_without_defined_output():
    with pytest.raises(AttributeError) as excinfo:
        upload.Strategy({'input': {}})


def test_strategy_prepare_template_params():
    expiration_ts_default = int(time.time() + 2592000)  # 30 days
    expiration_ts = int(time.time() + 90000)  # 1 day 1 hour
    sample_strategy_default = upload.Strategy({'input': {'text/csv': {}}, 'output': {}, 'collection': 'a.b'})
    sample_strategy = upload.Strategy(
        {'input': {'text/csv': {}}, 'templates': {'hash_of_segments': {'segment_separator': ':', 'retention': '1D1h'}},
         'output': {}, 'collection': 'a.b'})
    assert sample_strategy_default.template_params == {
        'hash_of_segments': {'expiration_ts': expiration_ts_default, 'segment_separator': ',',
                             'from_fields': ['segments']}}
    assert sample_strategy.template_params == {
        'hash_of_segments': {'expiration_ts': expiration_ts, 'segment_separator': ':', 'from_fields': ['segments']}}


def test_strategy_get_hash_of_segments():
    sample_strategy = upload.Strategy({'input': {'text/csv': {}}, 'output': {}, 'collection': 'a.b'})
    sample_strategy2 = upload.Strategy(
        {'input': {'text/csv': {}}, 'templates': {'hash_of_segments': {'retention': '5D2m4s'}}, 'output': {},
         'collection': 'a.b'})
    expiration_ts = int(time.time() + 2592000)  # 30 days
    expiration_ts2 = int(time.time() + 432124)  # 5D2m4s
    assert sample_strategy._get_hash_of_segments(['678269,678272,765488,408098']) == {'678269': expiration_ts,
                                                                                      '678272': expiration_ts,
                                                                                      '765488': expiration_ts,
                                                                                      '408098': expiration_ts}
    assert sample_strategy2._get_hash_of_segments(['2341,2452_4234']) == {'2341': expiration_ts2,
                                                                          '2452_4234': expiration_ts2}


def test_strategy_parse_output():
    sample_strategy = upload.Strategy({'input': {'text/csv': {}}, 'output': {}, 'collection': 'a.b'})
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
        'output': {'_id': "{{user_id}}", 'dmp': {'bk': '{{hash_of_segments}}'}}, 'collection': 'a.b'})
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
