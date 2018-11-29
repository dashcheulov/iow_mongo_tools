#!/usr/bin/env python3
# pylint: disable=line-too-long
""" Imports segments to mongo """
import os
import logging
import re
import subprocess
import gzip
import shutil
import time
import mimetypes
from copy import copy
from multiprocessing import Pool, Event
from iowmongotools import app, cluster, fs

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def run_command(args, cluster):  # deprecated
    """ **DEPRECATED**: Runs external command
    .. versionchanged:: 0.4
    Deprecated. Use :meth:`app.run_ext_command` instead.
    :returns exit code
    """
    logger.warning('Deprecated function \'iowmongotools.upload.run_command\' is being called')
    logger.info("Running command: %s", " ".join(args))
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1,
                            universal_newlines=True)
    with proc.stdout:
        for line in proc.stdout:
            logger.info('%s> %s', cluster, line.strip())
    return proc.wait()


def decompress_file(src, dst_dir):
    """ Decompresses file with gzip
    :returns abs path of decompressed file
    """
    if not src.endswith('.gz'):
        raise ValueError("%s unknown suffix -- ignored" % src)
    dst = os.path.join(dst_dir, os.path.basename(src.rsplit('.gz', 1)[0]))
    logger.info("Decompressing %s to %s", src, dst)
    with gzip.open(src, 'rb') as f_in:
        with open(dst, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    logger.debug("%s decompressed", src)
    return dst


class BadLine(ValueError):
    pass


class UnknownTemplate(ValueError):
    def __init__(self, name):
        super().__init__('Template \'%s\' is unknown.' % name)


class NoAnyDelivery(ValueError):
    def __init__(self, name):
        super().__init__('There is no known delivery for %s. Review config.' % name)


class WrongFileType(ValueError):
    def __init__(self, name, type, allowed_types):
        super().__init__('Type of file \'%s\' is \'%s\', expected %s' % (name, type, ' or '.join(allowed_types)))


class SegmentFile(object):  # pylint: disable=too-few-public-methods
    """ Represents file containing segments """
    SEPARATORS_MAP = {
        'text/tab-separated-values': '\t',
        'text/csv': ','
    }

    def __init__(self, path, provider, strategy):
        """
        :type path: str
        :param path: path to file
        :type config: app.Settings
        :param config: settings of application
        :type processor: callable
        :param processor: process file with segments, is invoked with following parameters filename, cluster, config

        :Example:
        ..code - block:: python

        def processor(filename, cluster, config):
            script_params = {
                'config': config.processing_script_config,
                'db': cluster,
                'mongo_timeout_seconds': str(config.mongo_timeout_seconds),
                'file': filename
            }
            return run_command(
                ['/usr/bin/perl', os.path.join(config.hg_scripts_dir, config.processing_script)] + [
                    '--{}={}'.format(k, v) for k, v in script_params.items()], cluster)
        segfile = SegmentFile(filename, config, processor)
        segfile.process(cluster)
        """

        class Flag(object):  # pylint: disable=too-few-public-methods
            """ Descriptor of flag """

            def __init__(self, name_in_db, init_value=None):
                self.name = name_in_db
                self.init_value = init_value
                self._val = None  # copy of value. For comparing we need to store previous value

            def __get__(self, obj, objtype):
                return obj.db.get(self.name) or self.init_value

            def __set__(self, obj, val):
                obj.db.update(self.name, val)
                if val != self._val:
                    obj.db.flush()
                    self._val = copy(val)

        class Flags(object):  # pylint: disable=too-few-public-methods,no-init
            """ Container for flags (properties in db) """
            db = object
            invalid = Flag('invalid', set())  # set of clusters, in which uploading failed
            processed = Flag('processed', set())  # set of clusters, in which uploading succeed

        if not isinstance(strategy, Strategy):
            raise TypeError('strategy should be an instance of class Strategy')
        self.path = path
        self.provider = provider
        self.name = os.path.basename(self.path).split('.')[0]
        self.strategy = strategy
        self.type = mimetypes.guess_type(path)
        if self.type[0] not in strategy.allowed_types:
            raise WrongFileType(self.name, self.type[0], strategy.allowed_types)
        self.tmp_file = self.path
        self.line_cnt = {'invalid': 0, 'total': 0}

    def __gt__(self, other):
        return os.stat(self.path).st_mtime > os.stat(other.path).st_mtime

    def get_line(self):
        if self.type[1] == 'gzip':
            open_func = gzip.open
            logger.debug('%s is type of %s. Opening with gzip.', self.name, self.type)
        else:
            open_func = open
            logger.debug('%s is type of %s. Opening.', self.name, self.type)
        with open_func(self.path, 'rt') as f_in:
            line = f_in.readline()
            while line:
                yield line.strip()
                line = f_in.readline()
            logger.debug('Closing %s', self.name)

    def get_setter(self, line_str):
        try:
            return self.strategy.get_setter(line_str.split(self.SEPARATORS_MAP[self.type[0]]),
                                            self.strategy.input[self.type[0]])
        except BadLine:
            raise BadLine('Line \'%s\' is invalid.' % line_str)


class Strategy(object):

    def __init__(self, config):
        if 'input' not in config or 'output' not in config:
            raise AttributeError('Uploading strategy must have both sections \'input\' and \'output\'')
        self.allowed_types = frozenset(ft for ft in SegmentFile.SEPARATORS_MAP.keys() if ft in config['input'])
        if not self.allowed_types:
            raise AttributeError(
                'Input must have at least one of type: %s' % ', '.join(SegmentFile.SEPARATORS_MAP.keys()))
        self.input = dict()
        for file_type in self.allowed_types:
            self.input[file_type] = {'titles': [], 'patterns': []}
            for title, pattern in config['input'][file_type].items():
                self.input[file_type]['titles'].append(title)
                self.input[file_type]['patterns'].append(re.compile(pattern))
        self.output = config['output']
        self.template_params = self.prepare_template_params(config.get('templates', dict()))
        self.lines_in_batch = config.get('lines_in_batch', 1000)
        self.threshold_invalid_lines_in_batch = config.get('threshold_invalid_lines_in_batch', 0.8)
        self.template = re.compile(r'{{(.*)}}')

    @staticmethod
    def prepare_template_params(config):
        if 'hash_of_segments' not in config:
            config['hash_of_segments'] = dict()
        config['hash_of_segments']['expiration_ts'] = int(
            time.time() + app.human_to_seconds(config['hash_of_segments'].pop('retention', '30D')))
        if 'segment_separator' not in config['hash_of_segments']:
            config['hash_of_segments']['segment_separator'] = ','
        return config

    def _get_hash_of_segments(self, segment_string):
        output = dict()
        for segment in segment_string.split(self.template_params['hash_of_segments']['segment_separator']):
            output[segment] = self.template_params['hash_of_segments']['expiration_ts']
        return output

    def get_setter(self, line, config):
        if len(config['titles']) != len(line):
            raise BadLine
        dict_line = dict()
        for index in range(len(config['titles'])):
            if not config['patterns'][index].match(line[index]):  # validation
                raise BadLine
            dict_line[config['titles'][index]] = line[index]
        return self._parse_output(self.output, dict_line)

    def _parse_output(self, item, dict_line):
        if isinstance(item, dict):
            item = item.copy()
            for key, value in item.items():
                item[key] = self._parse_output(value, dict_line)
            return item
        if isinstance(item, str):
            matched = self.template.match(item)
            if matched:
                return dict_line.get(matched.group(1)) or self._dispatch_template(matched.group(1), dict_line)
        return item

    def _dispatch_template(self, name, line):
        method_map = {
            'hash_of_segments': (self._get_hash_of_segments, 'segments')
        }
        if name not in method_map.keys():
            raise UnknownTemplate(name)
        return method_map[name][0](line.get(method_map[name][1]))


class FileEmitter(fs.EventHandler):

    def __init__(self, provider, upload_config):
        super().__init__()
        self.provider = provider
        self.errors = Event()
        self.delivery_config = upload_config.pop('delivery')
        self.strategy = Strategy(upload_config)
        logger.debug('Loaded strategy for %s', provider)
        self.start_observers()

    def on_file_discovered(self, path):
        logger.debug('%s is discovered. Put in queue', path)
        try:
            self.queue.put(SegmentFile(path, self.provider, self.strategy))
            self.items_ready.set()
        except WrongFileType as err:
            logger.error(err)
            self.errors.set()

    def start_observers(self):
        atleast_one_delivery_exists = False
        for delivery, config in self.delivery_config.items():
            if delivery not in fs.NAMES_MAP.keys():
                logger.warning('Don\'t know how to deliver \'%s\' from \'%s\'. Ignoring.', self.provider, delivery)
                continue
            logger.debug('Starting %s watcher for %s.', delivery, self.provider)
            getattr(fs, fs.NAMES_MAP[delivery])(self, config)
            atleast_one_delivery_exists = True
        if not atleast_one_delivery_exists:
            raise NoAnyDelivery(self.provider)


class Uploader(app.App, fs.EventHandler):
    def __init__(self):
        super().__init__()
        fs.EventHandler.__init__(self)
        self.pool = None
        self.results = []

    @property
    def default_config(self):
        config = super().default_config
        config.update({
            'upload': (dict(),),
            'db_path': ('db.yaml', 'Path to file containing state of uploading.'),
            'reprocess_invalid': (False, 'Whether reprocess files were not uploaded previously'),
        })
        return config

    def run(self):
        s = time.time()
        if not hasattr(self.config, 'clusters'):
            logger.error('Please provide cluster_config.yaml. See --help.')
            return 1
        for provider in self.config.upload.copy().keys():
            if provider not in self.config.providers:
                self.config.upload.pop(provider)
        if not self.config.upload:
            logger.error('Uploading isn\'t configured. Fill in section \'upload\' in config. Set \'--providers\'.')
            return 1
        mimetypes.init()
        mimetypes.types_map.update(self.config.mime_types_map)
        errors = len(self.config.clusters) - cluster.create_objects(self.config.clusters, self.config.cluster_config)
        self.pool = Pool(processes=len(cluster.Cluster.objects))
        file_emitters = [FileEmitter(provider, config) for provider, config in self.config.upload.items()]
        self.wait_for_items(file_emitters)
        self.consume_queue(file_emitters)
        while self.results:
            result_ready = False
            while not result_ready:  # polling of results
                time.sleep(0.5)
                for result in self.results:
                    if result.ready():
                        errors += result.get()
                        self.results.remove(result)
                        result_ready = True
                self.consume_queue(file_emitters)
            if not self.results:
                self.wait_for_items(file_emitters)
            self.consume_queue(file_emitters)
        logger.info('Total working time is %s', time.time() - s)
        for file_emitter in file_emitters:
            if file_emitter.errors.is_set():
                errors += 1
        return errors

    @staticmethod
    def wait_for_items(emitter_objects, timeout=10800, atleast_one=False):
        start_time = time.time()
        while time.time() - start_time < timeout:
            results = [obj.items_ready.is_set() for obj in emitter_objects]
            if atleast_one and any(results) or all(results):
                return True
            time.sleep(0.5)
        raise TimeoutError('Reached timeout while waiting for files.')

    def consume_queue(self, emitter_objects):
        for obj in emitter_objects:
            while not obj.queue.empty():
                self.results += [self.pool.apply_async(self.process_file, (cl_name, obj.queue.get())) for cl_name, _ in
                                 cluster.Cluster.objects.items()]

    @staticmethod
    def process_file(cluster_name, segfile):
        cl = cluster.Cluster.objects[cluster_name]
        logger.info('%s %s of type %s', cl.name, segfile.name, segfile.type)
        errors = 0
        ilb = 0  # counter of invalid lines in a batch
        batch = list()
        for line in segfile.get_line():
            segfile.line_cnt['total'] += 1
            try:
                batch.append(segfile.get_setter(line))
            except BadLine as err:
                logger.warning('%s,%s. %s', segfile.name, segfile.line_cnt['total'], err)
                ilb += 1
            if segfile.line_cnt['total'] % segfile.strategy.lines_in_batch == 0:
                if ilb / segfile.strategy.lines_in_batch > segfile.strategy.threshold_invalid_lines_in_batch:
                    logger.error('%s of %s lines in a batch are invalid. Marking \'%s\' as invalid',
                                 ilb, segfile.strategy.lines_in_batch, segfile.name)
                    errors += 1
                    break
                segfile.line_cnt['invalid'] += ilb
                ilb = 0
                logger.info(batch)
                batch = list()
        logger.info('Processed %s lines', segfile.line_cnt['total'])
        return errors
