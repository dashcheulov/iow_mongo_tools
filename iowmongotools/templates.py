#!/usr/bin/env python3
""" Templates of transformation a line from a segment file to a piece of json """
import time
import re
import sys
import importlib.machinery
from abc import ABC, abstractmethod
from iowmongotools import app


REGEXP = re.compile(r'{{([^}]+)}}')


def load_module(path):
    mod = importlib.machinery.SourceFileLoader(path.split('/')[-1].split('.')[0], path).load_module()
    for name, klass in mod.MAP.items():
        MAP[name] = klass
    sys.modules[mod.__name__] = mod


class Template(ABC):
    def __init__(self, config=None):
        pass

    @abstractmethod
    def apply(self, dict_line):
        raise NotImplementedError('You should implement this!')

    def push_filename(self, filename):
        pass


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
