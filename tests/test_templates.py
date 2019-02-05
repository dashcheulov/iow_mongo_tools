from iowmongotools import templates
import time


def test_hash_of_segments():
    template1 = templates.HashOfSegments()
    template2 = templates.HashOfSegments(
        {'retention': '5D2m4s', 'segment_separator': ':', 'segment_field_name': 'sg', 'path': 'ab'})
    expiration_ts = int(time.time() + 2592000)  # 30 days
    expiration_ts2 = int(time.time() + 432124)  # 5D2m4s
    assert template1.config == {'retention': 2592000, 'segment_separator': ',', 'segment_field_name': 'segments',
                                'path': None}
    assert template1.apply({'segments': '678269,678272,765488,408098'}) == {'678269': expiration_ts,
                                                                            '678272': expiration_ts,
                                                                            '765488': expiration_ts,
                                                                            '408098': expiration_ts}
    assert template2.config == {'retention': 432124, 'segment_separator': ':', 'segment_field_name': 'sg', 'path': 'ab'}
    assert template2.apply({'sg': '2341:2452_4234'}) == {'ab.2341': expiration_ts2,
                                                         'ab.2452_4234': expiration_ts2}


def test_segments_with_timestamp():
    template1 = templates.SegmentsWithTimestamp()
    template2 = templates.SegmentsWithTimestamp({'replacement_segment_separator': '|', 'segment_separator': ':',
                                                 'segment_field_name': 'sg',
                                                 'srting_pattern': '!{{timestamp}}{{timestamp_separator}}{{segments_string}}',
                                                 'timestamp_separator': '#'})
    ts = int(time.time())
    assert template1.apply({'segments': '678269,678272,765488,408098'}) == '678269,678272,765488,408098:%s' % ts
    assert template2.apply({'sg': '2341:2452_4234:234234'}) == '!%s#2341|2452_4234|234234' % ts


def test_timestamp():
    template = templates.Timestamp()
    current_ts = int(time.time())
    assert template.apply(None) == current_ts
