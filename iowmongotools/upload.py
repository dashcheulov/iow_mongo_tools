#!/usr/bin/env python3
# pylint: disable=line-too-long
""" Imports segments to mongo """
import os
import logging
import re
import gzip
import shutil
import time
import mimetypes
from multiprocessing import Pool, Event
from pymongo.operations import UpdateOne
from iowmongotools import app, cluster, fs

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


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


class InvalidSegmentFile(Exception):
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

    class Counter(object):
        def __init__(self):
            self.matched = 0
            self.modified = 0
            self.upserted = 0

        def __add__(self, other):
            obj = SegmentFile.Counter()
            for key in obj.__dict__.keys():
                obj.__dict__[key] = self.__dict__[key] + other.__dict__[key]
            return obj

        def __str__(self):
            out = list()
            for name, val in self.__dict__.items():
                if not val and name != 'matched':
                    continue
                out.append('{} - {}'.format(name, val))
            return 'Documents: {}.'.format(', '.join(out))

        def count_bulk_write_result(self, result):
            self.matched += result.matched_count
            if result.modified_count is not None:
                self.modified += result.modified_count
            self.upserted += result.upserted_count

    def __init__(self, path, provider, strategy):
        if not isinstance(strategy, Strategy):
            raise TypeError('strategy should be an instance of class Strategy')
        if not os.path.isfile(path):
            raise FileNotFoundError('File %s doesn\'t exist.' % path)
        self.logger = None
        self.path = path
        self.provider = provider
        self.name = os.path.basename(self.path).split('.')[0]
        self.strategy = strategy
        self.type = mimetypes.guess_type(path)
        if self.type[0] not in strategy.allowed_types:
            raise WrongFileType(self.name, self.type[0], strategy.allowed_types)
        self.invalid = False
        self.processed = False
        self.line_cnt = {'cur': 0, 'invalid': 0, 'total': 0}
        self.timer = Timer()
        self.counter = self.Counter()

    def __gt__(self, other):
        return os.stat(self.path).st_mtime > os.stat(other.path).st_mtime

    def get_line(self):
        if self.type[1] == 'gzip':
            open_func = gzip.open
            self.log('debug', 'The file is type of %s. Opening with gzip.'.format(self.type))
        else:
            open_func = open
            self.log('debug', 'The file is type of %s. Opening.'.format(self.type))
        with open_func(self.path, 'rt') as f_in:
            line = f_in.readline()
            while line:
                yield line.strip()
                line = f_in.readline()
            self.log('debug', 'Closing the file')

    def get_setter(self, line_str):
        try:
            return self.strategy.get_setter(line_str.split(self.SEPARATORS_MAP[self.type[0]]),
                                            self.strategy.input[self.type[0]])
        except BadLine:
            raise BadLine('Line \'%s\' is invalid.' % line_str)

    def get_batch(self):
        ilc = 0  # counter of invalid lines in a batch
        batch = list()
        self.invalid = False  # give it one more chance
        self.counter.__init__()
        self.timer.__init__()
        self.timer.start()
        self.processed = False
        last_log_ts = time.time()
        last_line_cnt = 0
        for line in self.get_line():
            self.line_cnt['cur'] += 1
            try:
                line = self.get_setter(line)
                batch.append(UpdateOne({'_id': line.pop('_id')}, line, upsert=self.strategy.upsert))
            except BadLine as err:
                if self.strategy.log_invalid_lines:
                    self.log('warning', '#{}. {}'.format(self.line_cnt['cur'], err))
                ilc += 1
            if self.line_cnt['cur'] % self.strategy.batch_size == 0:
                self.line_cnt['invalid'] += ilc
                if ilc / self.strategy.batch_size >= self.strategy.threshold_percent_invalid_lines_in_batch * 100:
                    self.invalid = True
                    self.timer.stop()
                    raise InvalidSegmentFile('%s of %s lines in a batch are invalid. Marking the file as invalid' %
                                             (ilc, self.strategy.batch_size))
                if time.time() - last_log_ts > 30:
                    speed = int((self.line_cnt['cur'] - last_line_cnt) / (time.time() - last_log_ts))
                    last_line_cnt = self.line_cnt['cur']
                    last_log_ts = time.time()
                    percent = '{:.0f}%'.format(self.line_cnt['cur'] * 100 / self.line_cnt['total']) if self.line_cnt[
                        'total'] else ''
                    self.log('info',
                             'Processing line #%-15s %-4s %10d lines/s' % (self.line_cnt['cur'], percent, speed))
                if batch:
                    yield batch
                ilc = 0
                batch = list()
        self.line_cnt['invalid'] += ilc
        if batch:
            self.log('debug', 'Line {}: {}'.format(self.line_cnt['cur'], batch[-1]))
            yield batch
        self.timer.stop()

    def load_metadata(self, data):
        if data:
            self.invalid = data.get('invalid', self.invalid)
            self.processed = data.get('processed', self.processed)
            self.timer.__dict__ = data.get('timer', self.timer.__dict__)
            self.counter.__dict__ = data.get('counter', self.counter.__dict__)
            if self.processed and not self.invalid:
                self.line_cnt['total'] = data['lines']['total']
            if self.provider != (data.get('provider') or self.provider):
                raise InvalidSegmentFile(
                    'File \'%s\' belonged to provider \'%s\'. Now you\'re trying to load it as \'%s\'' % (
                        self.name, data.get('provider'), self.provider))

    def dump_metadata(self):
        if not self.invalid:
            self.line_cnt['total'] = self.line_cnt['cur']
        return {
            '_id': self.name,
            'path': self.path,
            'provider': self.provider,
            'type': self.type,
            'invalid': self.invalid,
            'processed': self.processed,
            'lines': self.line_cnt,
            'timer': self.timer.__dict__,
            'counter': self.counter.__dict__
        }

    def log(self, severity, message):
        """ factory method using local or global logger. need in order not to pickle logger object to a processes """
        getattr(self.logger or logger, severity)(message)


class Strategy(object):

    def __init__(self, config):
        if 'input' not in config or 'update' not in config:
            raise AttributeError('Uploading strategy must have both sections \'input\' and \'update\'')
        if 'collection' not in config or len(config.get('collection', '').split('.')) != 2:
            raise AttributeError('Parameter \'collection\' is mandatory. Set as \'database.collection\'')
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
        self.output = config['update']
        if not '_id' in self.output:
            raise AttributeError('Section \'update\' must have field \'_id\'')
        self.database, self.collection = config['collection'].split('.')
        self.template_params = self.prepare_template_params(config.get('templates', dict()))
        self.batch_size = config.get('batch_size', 1000)
        self.reprocess_invalid = config.get('reprocess_invalid', False)
        self.force_reprocess = config.get('force_reprocess', False)
        self.upsert = config.get('upsert', False)
        self.log_invalid_lines = config.get('log_invalid_lines', True)
        self.threshold_percent_invalid_lines_in_batch = config.get('threshold_percent_invalid_lines_in_batch', 80)
        self.template = re.compile(r'{{(.*)}}')

    @staticmethod
    def prepare_template_params(config):
        config['hash_of_segments'] = {
            'segment_separator': config.get('hash_of_segments', dict()).get('segment_separator', ','),
            'expiration_ts': int(
                time.time() + app.human_to_seconds(config.get('hash_of_segments', dict()).pop('retention', '30D'))),
            'from_fields': config.get('hash_of_segments', dict()).get('from_fields', ['segments'])
        }
        return config

    def _get_hash_of_segments(self, fields):
        """

        :param fields: list of fields of line used in this function. Determined in parameter 'from_fields' in order.
        :return: generated hash of segments
        """
        output = dict()
        for segment in fields[0].split(self.template_params['hash_of_segments']['segment_separator']):
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
            'hash_of_segments': self._get_hash_of_segments
        }
        if name not in method_map.keys():
            raise UnknownTemplate(name)
        return method_map[name]([v for k, v in line.items() if k in self.template_params[name]['from_fields']])


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


class Timer(object):
    def __init__(self):
        self.started_ts = 0
        self.finished_ts = 0

    def start(self):
        self.started_ts = time.time()

    def stop(self):
        self.finished_ts = time.time()

    def __str__(self):
        if self.finished_ts > self.started_ts:
            return time.strftime("Processing time - %-H hours %-M minutes %-S seconds.",
                                 time.gmtime(self.finished_ts - self.started_ts))
        return ''


class Counter(object):

    def __init__(self):
        self.invalid = 0
        self.skipped = 0
        self._segfile_counters = list()

    def count_result(self, item):
        if item == 1:
            self.invalid += 1
        if item == 0:
            self.skipped += 1
        if isinstance(item, SegmentFile.Counter):
            self._segfile_counters.append(item)

    def __str__(self):
        out = list()
        for name, val in self.__dict__.items():
            if not val:
                continue
            if name == '_segfile_counters':
                out.append('{} - {}'.format('processed', len(val)))
                continue
            out.append('{} - {}'.format(name, val))
        segfile_counters = sum(self._segfile_counters, self._segfile_counters.pop()) if self._segfile_counters else ''
        return 'Total files: {}. {}'.format(', '.join(out), segfile_counters)


class Uploader(app.App):
    def __init__(self):
        super().__init__()
        self.pool = None
        self.results = []

    @property
    def default_config(self):
        config = super().default_config
        config.update({
            'upload': (dict(),),
            'reprocess_invalid': (False, 'Whether reprocess files were not uploaded previously'),
            'reprocess_file': ([], 'Paths of files which will be reprocessed.'),
            'force': (False, 'Process files even they have been processed successfully previously'),
            'segments_collection': ('', 'Full name of collection (\'database.collection\') for uploading segments')
        })
        config['logging'] = (app.deep_merge(config['logging'][0], {'formatters': {
            'worker': {
                'format': '%(asctime)s %(cluster)s %(provider)s [%(segfile)s] %(levelname)s: %(message)s',
                'datefmt': '%H:%M:%S'}},
            'handlers': {
                'worker': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'worker',
                    'stream': 'ext://sys.stdout'}},
            'loggers': {
                'worker': {
                    '()': 'ConstantExtraLogger',
                    'handlers': ['worker'],
                    'propagate': False}}}),)
        return config

    def run(self):
        timer = Timer()
        timer.start()
        if not hasattr(self.config, 'clusters'):
            logger.error('Please provide cluster_config.yaml. See --help.')
            return 1
        mimetypes.init()
        mimetypes.types_map.update(self.config.mime_types_map)
        for key in self.config.upload.keys():  # merge providers' settings with global ones
            if 'reprocess_invalid' not in self.config.upload[key]:
                self.config.upload[key]['reprocess_invalid'] = self.config.reprocess_invalid
            if 'force_reprocess' not in self.config.upload[key]:
                self.config.upload[key]['force_reprocess'] = self.config.force
            self.config.upload[key]['collection'] = self.config.upload[key].get(
                'collection') or self.config.segments_collection
        errors = len(self.config.clusters) - cluster.create_objects(self.config.clusters, self.config.cluster_config)
        self.pool = Pool(processes=len(cluster.Cluster.objects))  # forking
        if self.config.reprocess_file:  # reprocessing given paths. We don't need to discover files
            if len(self.config.providers) != 1:
                logger.error('You\'re using --reprocess_file, please set only one of \'%s\' provider with --providers',
                             ', '.join(self.config.upload.keys()))
                return 1
            for path in self.config.reprocess_file:
                try:
                    segfile = SegmentFile(path, self.config.providers[0],
                                          Strategy(self.config.upload[self.config.providers[0]]))
                    self.results += [self.pool.apply_async(self.process_file, (cl_name, segfile))
                                     for cl_name, _ in cluster.Cluster.objects.items()]
                except FileNotFoundError as err:
                    logger.error(err)
                    errors += 1
            for result in self.results:
                errors += result.get()
            return errors  # end of reprocessing paths.
        for provider in self.config.upload.copy().keys():
            if provider not in self.config.providers:
                self.config.upload.pop(provider)
        if not self.config.upload:
            logger.error('Uploading isn\'t configured. Fill in section \'upload\' in config. Set \'--providers\'.')
            return 1
        file_emitters = [FileEmitter(provider, config) for provider, config in self.config.upload.items()]
        self.wait_for_items(file_emitters)
        self.consume_queue(file_emitters)
        counter = Counter()
        while self.results:
            result_ready = False
            while not result_ready:  # polling of results
                time.sleep(0.5)
                for result in self.results:
                    if result.ready():
                        counter.count_result(result.get())
                        self.results.remove(result)
                        result_ready = True
                self.consume_queue(file_emitters)
            if not self.results:
                self.wait_for_items(file_emitters)
            self.consume_queue(file_emitters)
        for file_emitter in file_emitters:
            if file_emitter.errors.is_set():
                errors += 1
        timer.stop()
        logger.info('%s %s', counter, timer)
        return errors + counter.invalid

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
        logger = logging.getLogger('worker')
        logger.extra = {'provider': segfile.provider, 'segfile': segfile.name, 'cluster': cl.name}
        segfile.logger = logger
        try:
            cl.read_segfile_info(segfile)
        except InvalidSegmentFile as err:
            logger.error(err)
            return 1
        if segfile.processed and not segfile.invalid and not segfile.strategy.force_reprocess:
            logger.debug('The file has already been uploaded. Skipping.')
            return 0
        if segfile.invalid and not segfile.strategy.reprocess_invalid and not segfile.strategy.force_reprocess:
            logger.debug('The file is invalid. Skipping.')
            return 0
        if segfile.invalid:
            logger.info('The file was invalid. Reprocessing.')
        elif segfile.processed:
            logger.info('The file was uploaded successfully at %s. Reprocessing.',
                        time.strftime('%d %b %Y %H:%M', time.localtime(segfile.timer.finished_ts)))
        else:
            logger.info('Starting uploading file \'%s\'.', segfile.path)
        try:
            cl.upload_segfile(segfile)
        except InvalidSegmentFile as err:
            logger.error(err)
            return 1
        else:
            segfile.processed = True
        finally:
            cl.save_segfile_info(segfile)
            logger.info('Finished %s lines from %s, %s invalid lines. %s %s', segfile.line_cnt['cur'],
                        segfile.path, segfile.line_cnt['invalid'], segfile.counter, segfile.timer)
        return segfile.counter
