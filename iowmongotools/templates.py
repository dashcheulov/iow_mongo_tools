#!/usr/bin/env python3
""" Templates of transformation a line from a segment file to a piece of json """
import time
import re
from abc import ABC, abstractmethod
from iowmongotools import app


REGEXP = re.compile(r'{{([^}]+)}}')


class Template(ABC):
    def __init__(self, config=None):
        pass

    @abstractmethod
    def apply(self, dict_line):
        raise NotImplementedError('You should implement this!')


class HashOfSegments(Template):
    def __init__(self, config=None):
        if not config:
            config = dict()
        self.config = {
            'segment_separator': config.get('segment_separator', ','),
            'retention': app.human_to_seconds(config.get('retention', '30D')),
            'segment_field_name': config.get('segment_field_name', 'segments')
        }

    def apply(self, dict_line):
        output = dict()
        for segment in dict_line[self.config['segment_field_name']].split(self.config['segment_separator']):
            output[segment] = int(time.time() + self.config['retention'])
        return output


class Timestamp(Template):
    def apply(self, dict_line):
        return int(time.time())


MAP = {
    'hash_of_segments': HashOfSegments,
    'timestamp': Timestamp
}


# def _resub(self, line):
#     def _substitute(match_obj):
#         return dict_line.get(match_obj.group(1)) or self._dispatch_template(match_obj.group(1), dict_line)
#
#     item = templates.REGEXP.sub(_substitute, item)