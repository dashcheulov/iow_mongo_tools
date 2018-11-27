from iowmongotools import upload
import pytest
import time


def test_strategy_without_defined_output():
    with pytest.raises(AttributeError) as excinfo:
        upload.Strategy({'input': {}})


def test_strategy_prepare_template_params():
    expiration_ts_default = int(time.time() + 2592000)  # 30 days
    expiration_ts = int(time.time() + 90000)  # 1 day 1 hour
    sample_strategy_default = upload.Strategy({'input': {}, 'output': {}})
    sample_strategy = upload.Strategy(
        {'input': {'templates': {'hash_of_segments': {'segment_separator': ':', 'retention': '1D1h'}}}, 'output': {}})
    assert sample_strategy_default.template_params == {
        'hash_of_segments': {'expiration_ts': expiration_ts_default, 'segment_separator': ','}}
    assert sample_strategy.template_params == {
        'hash_of_segments': {'expiration_ts': expiration_ts, 'segment_separator': ':'}}


def test_strategy_get_hash_of_segments():
    sample_strategy = upload.Strategy({'input': {}, 'output': {}})
    sample_strategy2 = upload.Strategy(
        {'input': {'templates': {'hash_of_segments': {'retention': '5D2m4s'}}}, 'output': {}})
    expiration_ts = int(time.time() + 2592000)  # 30 days
    expiration_ts2 = int(time.time() + 432124)  # 5D2m4s
    assert sample_strategy.get_hash_of_segments('678269,678272,765488,408098') == {'678269': expiration_ts,
                                                                                   '678272': expiration_ts,
                                                                                   '765488': expiration_ts,
                                                                                   '408098': expiration_ts}
    assert sample_strategy2.get_hash_of_segments('2341,2452_4234') == {'2341': expiration_ts2,
                                                                       '2452_4234': expiration_ts2}
