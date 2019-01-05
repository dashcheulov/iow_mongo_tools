#!/usr/bin/env python3
# pylint: disable=line-too-long
""" Imports segments to mongo """
import os
import logging
import re
import gzip
import time
from collections import deque
from copy import copy
import mimetypes
from multiprocessing import Pool, Event, Array
from pymongo.operations import UpdateOne
from iowmongotools import app, cluster, fs, templates

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


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


class SegmentFile(object):
    """ Represents file containing segments """
    SEPARATORS_MAP = {
        'text/tab-separated-values': '\t',
        'text/csv': ','
    }

    class Counter(object):
        def __init__(self, line_total=0):
            self.matched = 0
            self.modified = 0
            self.upserted = 0
            self.line_cur = 0
            self.line_invalid = 0
            self.line_total = line_total

        def __add__(self, other):
            obj = SegmentFile.Counter()
            for key in obj.__dict__.keys():
                obj.__dict__[key] = self.__dict__[key] + other.__dict__[key]
            return obj

        def __and__(self, other):
            for key in ('matched', 'modified', 'upserted'):
                self.__dict__[key] += other.__dict__[key]
            return self

        def __str__(self):
            docs = list()
            for name, val in self.__dict__.items():
                if not val and name != 'matched' or 'line' in name:
                    continue
                docs.append('{} - {}'.format(name, val))
            return 'Lines: total - {}, invalid - {}. Documents: {}.'.format(self.line_total, self.line_invalid,
                                                                            ', '.join(docs))

        def count_bulk_write_result(self, result):
            if result.acknowledged:
                self.matched += result.matched_count
                if result.modified_count is not None:
                    self.modified += result.modified_count
                self.upserted += result.upserted_count

    def __init__(self, path, provider, strategy):
        if not isinstance(strategy, Strategy):
            raise TypeError('strategy should be an instance of class Strategy')
        if not os.path.isfile(path):
            raise FileNotFoundError('File {} doesn\'t exist.'.format(path))
        self.logger = None
        self.shared_index = None
        self.shared_metrics = [0, 0]
        self.path = path
        self.provider = provider
        self.strategy = strategy
        if self.strategy.override_filename_from_path:
            regexp, replacement = self.strategy.override_filename_from_path
            self.name = regexp.sub(replacement, self.path)
        else:
            self.name = os.path.basename(self.path).split('.')[0]
        self.type = strategy.get_file_type(path)
        if self.type[0] not in strategy.allowed_types:
            raise WrongFileType(self.name, self.type[0], strategy.allowed_types)
        self.invalid = False
        self.processed = False
        self.timer = Timer()
        self.counter = self.Counter()

    def __gt__(self, other):
        return os.stat(self.path).st_mtime > os.stat(other.path).st_mtime

    def get_line(self):
        if self.type[1] == 'gzip':
            open_func = gzip.open
            self.log('debug', 'The file is type of {}. Opening with gzip.'.format(self.type))
        else:
            open_func = open
            self.log('debug', 'The file is type of {}. Opening.'.format(self.type))
        with open_func(self.path, 'rt') as f_in:
            line = f_in.readline()
            if self.type[0] == 'text/csv':
                try:
                    self.get_setter(line)
                except BadLine:
                    self.log('debug', 'Seems the first line is header of csv. Skipping.')
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
            raise BadLine('Line \'{}\' is invalid.'.format(line_str))

    def get_batch(self):
        ilc = 0  # counter of invalid lines in a batch
        batch = list()
        self.populate_templates_with_filename()
        self.invalid = False  # give it one more chance
        self.counter.__init__(self.counter.line_total)
        self.timer.__init__()
        self.timer.start()
        self.processed = False
        last_log_ts = time.time()
        last_line_cnt = 0
        for line in self.get_line():
            self.counter.line_cur += 1
            try:
                line = self.get_setter(line)
                batch.append(UpdateOne({'_id': line.pop('_id')}, line, upsert=self.strategy.upsert))
            except BadLine as err:
                if self.strategy.log_invalid_lines:
                    self.log('warning', '#{}. {}'.format(self.counter.line_cur, err))
                ilc += 1
            if self.counter.line_cur % self.strategy.batch_size == 0:
                self.counter.line_invalid += ilc
                self.shared_metrics[1] += ilc
                self.shared_metrics[0] += self.strategy.batch_size
                if not self.invalid and ilc / self.strategy.batch_size >= self.strategy.threshold_percent_invalid_lines_in_batch / 100:
                    self.invalid = True
                    self.log('error', '{} of {} lines in a batch are invalid. Marking the file as invalid'.format(ilc,
                                                                                                                  self.strategy.batch_size))
                    if not self.strategy.process_invalid_file_to_end:
                        self.timer.stop()
                        raise InvalidSegmentFile('Stop processing the file')
                    self.log('info',
                             'Option \'process_invalid_file_to_end\' is enabled. Switching off logging of invalid lines and going on processing the file.')
                    self.strategy.log_invalid_lines = False
                if time.time() - last_log_ts > 30:
                    speed = int((self.counter.line_cur - last_line_cnt) / (time.time() - last_log_ts))
                    last_line_cnt = self.counter.line_cur
                    last_log_ts = time.time()
                    if self.shared_index and not self.counter.line_total:
                        if Uploader.shared_array[self.shared_index]:
                            self.counter.line_total = Uploader.shared_array[self.shared_index]
                    percent = '{:.0f}%'.format(
                        self.counter.line_cur * 100 / self.counter.line_total) if self.counter.line_total else ''
                    self.log('info',
                             'Processing line #%-15s %-4s %10d lines/s' % (self.counter.line_cur, percent, speed))
                if batch:
                    yield batch
                ilc = 0
                batch = list()
        self.counter.line_invalid += ilc
        self.shared_metrics[1] += ilc
        self.shared_metrics[0] += self.counter.line_cur % self.strategy.batch_size
        if batch:
            self.log('debug', 'Line {}: {}'.format(self.counter.line_cur, batch[-1]))
            yield batch
        if self.strategy.process_invalid_file_to_end or not self.invalid:
            self.counter.line_total = self.counter.line_cur
            if self.shared_index:
                Uploader.shared_array[self.shared_index] = self.counter.line_total
        self.timer.stop()

    def load_metadata(self, data):
        if data:
            self.invalid = data.get('invalid', self.invalid)
            self.processed = data.get('processed', self.processed)
            self.timer.__dict__.update(data.get('timer', {}))
            self.counter.__dict__.update(data.get('counter', {}))
            if not self.processed or self.invalid:
                self.counter.line_total = 0
            if self.provider != (data.get('provider') or self.provider):
                raise InvalidSegmentFile(
                    'File \'%s\' is belonged to provider \'%s\'. Now you\'re trying to load it as \'%s\'' % (
                        self.name, data.get('provider'), self.provider))
            if self.shared_index and self.counter.line_total:
                Uploader.shared_array[self.shared_index] = self.counter.line_total

    def dump_metadata(self):
        timer = self.timer.__dict__.copy()
        timer.pop('_Timer__scheduler_ts')
        return {
            '_id': self.name,
            'path': self.path,
            'provider': self.provider,
            'type': self.type,
            'invalid': self.invalid,
            'processed': self.processed,
            'timer': timer,
            'counter': self.counter.__dict__
        }

    def log(self, severity, message):
        """ factory method using local or global logger. need in order not to pickle logger object to a processes """
        getattr(self.logger or logger, severity)(message)

    def populate_templates_with_filename(self):
        for template in self.strategy.templates.values():
            template.push_filename(self.name)


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
        if '_id' not in self.output:
            raise AttributeError('Section \'update\' must have field \'_id\'')
        self.database, self.collection = config['collection'].split('.')
        templates_params = config.get('templates', dict())
        self.templates = dict()
        for name in set(templates.MAP.keys()).intersection(self.set_of_used_templates(self.output)):
            self.templates[name] = templates.MAP[name](templates_params.get(name))
        self.batch_size = config.get('batch_size', 1000)
        self.reprocess_invalid = config.get('reprocess_invalid', False)
        self.force_reprocess = config.get('force_reprocess', False)
        self.process_invalid_file_to_end = config.get('process_invalid_file_to_end', True)
        self.upsert = config.get('upsert', False)
        self.__file_type_override = config.get('file_type_override', None)
        self.log_invalid_lines = config.get('log_invalid_lines', True)
        self.threshold_percent_invalid_lines_in_batch = config.get('threshold_percent_invalid_lines_in_batch', 80)
        self.override_filename_from_path = config.get('override_filename_from_path')
        if isinstance(self.override_filename_from_path, dict):
            pattern, replacement = next(iter(self.override_filename_from_path.items()))
            self.override_filename_from_path = re.compile(pattern), replacement
        self.write_concern = config.get('write_concern')

    def get_setter(self, line, config):
        if len(config['titles']) != len(line):
            raise BadLine
        dict_line = dict()
        for index in range(len(config['titles'])):
            if not config['patterns'][index].match(line[index]):  # validation
                raise BadLine
            dict_line[config['titles'][index]] = line[index].strip()
        return self._parse_output(self.output, dict_line)

    def _parse_output(self, item, dict_line):
        if isinstance(item, dict):
            item = item.copy()
            for key, value in item.items():
                item[key] = self._parse_output(value, dict_line)
            return item
        if isinstance(item, str):
            matched = templates.REGEXP.match(item)
            if matched:
                return dict_line.get(matched.group(1)) or self._dispatch_template(matched.group(1), dict_line)
        return item

    def _dispatch_template(self, name, dict_line):
        if name not in self.templates.keys():
            raise UnknownTemplate(name)
        return self.templates[name].apply(dict_line)

    def set_of_used_templates(self, item):
        if isinstance(item, dict):
            out = set()
            for value in item.values():
                out.update(self.set_of_used_templates(value))
            return out
        if isinstance(item, str):
            matched = templates.REGEXP.match(item)
            if matched:
                return {matched.group(1)}

    def get_file_type(self, path):
        rtype = mimetypes.guess_type(path)
        return (self.__file_type_override, rtype[1]) if self.__file_type_override else rtype


class FileEmitter(fs.EventHandler):
    class Sorter(object):
        def __init__(self, config):
            if 'file_path_regexp' not in config or 'order' not in config:
                raise AttributeError('Section \'sorting\' must have \'file_path_regexp\' and \'order\'')
            self.file_path_regexp = re.compile(config['file_path_regexp'])
            self.order = config['order']

        def _get_variables(self, path):
            stat = os.stat(path)
            stat_dict = dict()
            for field in ('st_size', 'st_atime', 'st_mtime', 'st_ctime'):
                stat_dict[field] = getattr(stat, field)
            try:
                matched = self.file_path_regexp.match(path).groups()
            except AttributeError:
                raise InvalidSegmentFile('File {} doesn\'t match to sorting regexp'.format(path))
            return {'origin': path, 'path': matched, 'stat': stat_dict}

        def sort(self, files):
            lst = list()
            for path in files:
                lst.append(self._get_variables(path))
            for rule in self.order[::-1]:
                for field, order in rule.items():
                    field = field.split('.')
                    try:
                        field[1] = int(field[1])
                    except ValueError:
                        pass
                    lst.sort(key=lambda x: x[field[0]][field[1]], reverse=(True if order == 'desc' else False))
            return [f['origin'] for f in lst]

    def __init__(self, provider, upload_config):
        super().__init__()
        self.provider = provider
        self.errors = Event()
        self.delivery_config = upload_config.pop('delivery')
        self.sorting = self.Sorter(upload_config['sorting']) if 'sorting' in upload_config else None
        self.sort = self.sorting.sort if self.sorting else lambda x: x
        self.strategy = Strategy(upload_config)
        logger.debug('Loaded strategy for %s', provider)

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
        self.__scheduler_ts = dict()

    def start(self):
        self.started_ts = time.time()

    def stop(self):
        self.finished_ts = time.time()

    def execute(self, func, args, interval):
        signature = func.__name__, args
        ts = time.time()
        if ts >= self.__scheduler_ts.get(signature, 0) + interval:
            self.__scheduler_ts[signature] = ts
            return func(*args)

    def __str__(self):
        if self.finished_ts > self.started_ts:
            return time.strftime("Processing time - %-H hours %-M minutes %-S seconds.",
                                 time.gmtime(self.finished_ts - self.started_ts))
        return ''


class Counter(object):

    def __init__(self):
        self._segfile_counters = dict()
        self._invalid_files = set()
        self._skipped_files = set()

    @property
    def invalid(self):
        return len(self._invalid_files)

    @property
    def skipped(self):
        return len(self._skipped_files)

    @property
    def processed(self):
        processed = set()
        for item in self._segfile_counters.keys():
            processed.add(item[0])
        return len(processed)

    @property
    def known_providers(self):
        known_providers = set()
        for item in self._segfile_counters.keys():
            known_providers.add(item[1])
        return known_providers

    @property
    def known_clusters(self):
        known_clusters = set()
        for item in self._segfile_counters.keys():
            known_clusters.add(item[2])
        return known_clusters

    def flush_metrics(self, prefix, path, from_shared_memory=True):
        out = []
        ts = int(time.time())
        if from_shared_memory:
            for provider in Uploader.shared_metrics.keys():
                for cl in Uploader.shared_metrics[provider].keys():
                    mp = (('lines_processed', Uploader.shared_metrics[provider][cl][0]), ('uploaded',
                                                                                          Uploader.shared_metrics[
                                                                                              provider][cl][0] -
                                                                                          Uploader.shared_metrics[
                                                                                              provider][cl][1]))
                    for metric in mp:
                        out.append('{}.{}.{}.{} {} {}\n'.format(prefix, provider, cl, *metric, ts))
        else:
            for provider in self.known_providers:
                for cl in self.known_clusters:
                    counter = self._aggregate_counters((provider,), (cl,))
                    if not counter:
                        return None
                    mp = (('lines_processed', counter.line_total),
                          ('uploaded', counter.line_total - counter.line_invalid))
                    for metric in mp:
                        out.append('{}.{}.{}.{} {} {}\n'.format(prefix, provider, cl, *metric, ts))
        logger.debug('Flushing %s metrics to %s', len(out), path)
        with open(path, 'a+') as metrics_file:
            metrics_file.write(''.join(out))

    def _aggregate_counters(self, providers=None, clusters=None):
        """
        Sum all _segfile_counters by providers or/and clusters
        :return: united object of SegmentFile.Counter or empty str
        """
        if not self._segfile_counters:
            return ''
        if not providers:
            providers = self.known_providers
        if not clusters:
            clusters = self.known_clusters
        filtered_keys = set()
        known_files = set()
        out = list()
        for key in self._segfile_counters.keys():
            if key[1] in providers and key[2] in clusters:
                filtered_keys.add(key)
        for key in filtered_keys:
            if key[0] in known_files:
                out[-1] &= self._segfile_counters[key]
                continue
            out.append(copy(self._segfile_counters[key]))
            known_files.add(key[0])
        if len(out) >= 2:
            return sum(out[:-1], out[-1])
        if not out:
            return None
        return out[0]

    def count_result(self, item):
        signature = item[0], item[3], item[4]  # slice of filename, provider, cluster_name
        if item[1] == 1:
            self._invalid_files.add(item[0])
        if item[1:3] == (0, None) and signature not in self._segfile_counters:
            self._skipped_files.add(item[0])
        if isinstance(item[2], SegmentFile.Counter):
            if signature not in self._segfile_counters:
                self._segfile_counters[signature] = item[2]
            self._skipped_files.discard(item[0])

    def __str__(self):
        out = list()
        for name in ('processed', 'invalid', 'skipped'):
            if not getattr(self, name) and name != 'processed':
                continue
            out.append('{} - {}'.format(name, getattr(self, name)))
        return 'Total files: {}. {}'.format(', '.join(out), self._aggregate_counters())


class Uploader(app.App):
    shared_array = Array('i', 1000)
    shared_metrics = dict()

    def __init__(self):
        super().__init__()
        self.pool = None
        self.results = []
        self.buffer = dict()
        self.counter = Counter()
        mimetypes.init()

    @property
    def default_config(self):
        config = super().default_config
        config.update({
            'upload': (dict(),),
            'reprocess_invalid': (False, 'Whether reprocess files were not uploaded previously'),
            'reprocess_file': ([], 'Paths of files which will be reprocessed.'),
            'force': (False, 'Process files even if they have been processed successfully previously'),
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
        if hasattr(self.config, 'mime_types_map'):
            mimetypes.types_map.update(self.config.mime_types_map)
        if hasattr(self.config, 'module_templates'):
            logger.info('Loading external templates from %s', self.config.module_templates)
            templates.load_module(self.config.module_templates)
        if not self.config.upload:
            logger.error('Uploading isn\'t configured. Fill in section \'upload\' in config. Set \'--providers\'.')
            return 1
        if hasattr(self.config, 'metrics'):
            if 'prefix' not in self.config.metrics or 'path' not in self.config.metrics:
                raise AttributeError('Config of \'metrics\' must contain \'prefix\' and \'path\'')
            if 'flush_interval' not in self.config.metrics:
                self.config.metrics['flush_interval'] = 60
        for provider in self.config.upload.copy().keys():
            if provider not in self.config.providers:
                self.config.upload.pop(provider)
        for provider in self.config.providers:
            if provider not in self.config.upload.copy().keys():
                logger.error('Unknown provider %s. Fill in section \'upload\' in config.', provider)
                return 1
        for key in self.config.upload.keys():  # merge providers' settings with global ones
            if 'reprocess_invalid' not in self.config.upload[key]:
                self.config.upload[key]['reprocess_invalid'] = self.config.reprocess_invalid
            if 'force_reprocess' not in self.config.upload[key]:
                self.config.upload[key]['force_reprocess'] = self.config.force
            self.config.upload[key]['collection'] = self.config.upload[key].get(
                'collection') or self.config.segments_collection
        for key in self.config.cluster_config.keys():
            if 'mongo_client_settings' not in self.config.cluster_config[key] and hasattr(self.config,
                                                                                          'mongo_client_settings'):
                self.config.cluster_config[key]['mongo_client_settings'] = self.config.mongo_client_settings
        errors = len(self.config.clusters) - cluster.create_objects(self.config.clusters, self.config.cluster_config)
        for provider in self.config.providers:  # init buffer
            self.buffer[provider] = dict()
        for provider in self.config.providers:  # files of one provider can't be uploaded to a cluster simultaneously
            self.shared_metrics[provider] = dict()
            for cl in self.config.clusters:
                self.buffer[provider][cl] = [True, deque()]
                self.shared_metrics[provider][cl] = Array('i', 2)  # array of current_line, invalid_lines

        def init(shared_array, shared_metrics):
            Uploader.shared_array = shared_array
            Uploader.shared_metrics = shared_metrics

        self.pool = Pool(processes=self.config.workers, initializer=init,
                         initargs=(self.shared_array, self.shared_metrics))  # forking
        if self.config.reprocess_file:  # reprocessing given paths. We don't need to discover files
            if len(self.config.providers) != 1:
                logger.error('You\'re using --reprocess_file, please set only one of \'%s\' provider with --providers',
                             ', '.join(self.config.upload.keys()))
                return 1
            file_emitter = FileEmitter(self.config.providers[0], self.config.upload[self.config.providers[0]])
            for path in self.config.reprocess_file:
                file_emitter.on_file_discovered(path)
            return self.main(errors, (file_emitter,), timer)  # end of reprocessing paths.
        file_emitters = [FileEmitter(provider, config) for provider, config in self.config.upload.items()]
        for file_emitter in file_emitters:
            file_emitter.start_observers()
        self.wait_for_items(file_emitters)
        return self.main(errors, file_emitters, timer)

    def main(self, errors, file_emitters, timer):
        self.consume_queue(file_emitters)
        while self.results:
            result_ready = False
            while not result_ready:  # polling of results
                time.sleep(0.01)
                for result in self.results:
                    if result.ready():
                        self.handle_result(result.get())
                        self.results.remove(result)
                        result_ready = True
                        self.consume_queue(file_emitters)
                timer.execute(self.counter.flush_metrics, (self.config.metrics['prefix'], self.config.metrics['path']),
                              self.config.metrics['flush_interval'])
            if not self.results:
                self.wait_for_items(file_emitters)
            self.consume_queue(file_emitters)
        for file_emitter in file_emitters:
            if file_emitter.errors.is_set():
                errors += 1
        timer.stop()
        self.counter.flush_metrics(self.config.metrics['prefix'], self.config.metrics['path'])
        logger.info('%s %s', self.counter, timer)
        return errors + self.counter.invalid

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
        for obj in emitter_objects:  # check emitter queues and put objects to buffer
            while not obj.queue.empty():
                segment_file = obj.queue.get()
                Uploader.shared_array[0] = (Uploader.shared_array[0] + 1) % 1000  # increase index pointer within 1000
                if not Uploader.shared_array[0]:  # index pointer mustn't point to itself
                    Uploader.shared_array[0] += 1
                Uploader.shared_array[Uploader.shared_array[0]] = 0  # init element
                segment_file.shared_index = Uploader.shared_array[0]  # pass index to segment_file
                for cl_name in cluster.Cluster.objects.keys():
                    self.buffer[segment_file.provider][cl_name][1].append(segment_file)
        for provider in self.config.providers:  # check buffer
            for cl in cluster.Cluster.objects.keys():
                if self.buffer[provider][cl][0] and self.buffer[provider][cl][1]:
                    self.results.append(
                        self.pool.apply_async(self.process_file, (cl, self.buffer[provider][cl][1].popleft())))
                    self.buffer[provider][cl][0] = False

    def handle_result(self, result):
        """
        :param result: tuple of segfile.name, err_code, segfile.counter, segfile.provider, cluster.name
        """
        self.counter.count_result(result)
        self.buffer[result[3]][result[4]][0] = True

    @staticmethod
    def process_file(cluster_name, segfile):
        """
        :return: (error_code, counter)
        """
        cl = cluster.Cluster.objects[cluster_name]
        logger = logging.getLogger('worker')
        logger.extra = {'provider': segfile.provider, 'segfile': segfile.name, 'cluster': cl.name}
        segfile.logger = logger
        segfile.shared_metrics = Uploader.shared_metrics[segfile.provider][cl.name]
        try:
            cl.read_segfile_info(segfile)
        except InvalidSegmentFile as err:
            logger.error(err)
            return segfile.name, 1, None, segfile.provider, cl.name
        if segfile.processed and not segfile.invalid and not segfile.strategy.force_reprocess:
            logger.debug('The file has already been uploaded. Skipping.')
            return segfile.name, 0, None, segfile.provider, cl.name
        if segfile.invalid and not segfile.strategy.reprocess_invalid and not segfile.strategy.force_reprocess:
            logger.debug('The file is invalid. Skipping.')
            return segfile.name, 0, None, segfile.provider, cl.name
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
            return segfile.name, 1, segfile.counter, segfile.provider, cl.name
        else:
            segfile.processed = True
        finally:
            cl.save_segfile_info(segfile)
            logger.info('Finished %s. %s %s', segfile.path, segfile.counter, segfile.timer)
        return segfile.name, 1 if segfile.invalid else 0, segfile.counter, segfile.provider, cl.name
