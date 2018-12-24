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
from iowmongotools import app, cluster, fs, templates

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
            self.line_cur = 0
            self.line_invalid = 0
            self.line_total = 0

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
        self.path = path
        self.provider = provider
        self.name = os.path.basename(self.path).split('.')[0]
        self.strategy = strategy
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
        self.counter.__init__()
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
                    percent = '{:.0f}%'.format(
                        self.counter.line_cur * 100 / self.counter.line_total) if self.counter.line_total else ''
                    self.log('info',
                             'Processing line #%-15s %-4s %10d lines/s' % (self.counter.line_cur, percent, speed))
                if batch:
                    yield batch
                ilc = 0
                batch = list()
        self.counter.line_invalid += ilc
        if batch:
            self.log('debug', 'Line {}: {}'.format(self.counter.line_cur, batch[-1]))
            yield batch
        if self.strategy.process_invalid_file_to_end or not self.invalid:
            self.counter.line_total = self.counter.line_cur
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
                    'File \'%s\' belonged to provider \'%s\'. Now you\'re trying to load it as \'%s\'' % (
                        self.name, data.get('provider'), self.provider))

    def dump_metadata(self):
        return {
            '_id': self.name,
            'path': self.path,
            'provider': self.provider,
            'type': self.type,
            'invalid': self.invalid,
            'processed': self.processed,
            'timer': self.timer.__dict__,
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
        return len(self._segfile_counters)

    @property
    def segfile_counters(self):
        if not self._segfile_counters:
            return ''
        first = self._segfile_counters.pop(next(iter(self._segfile_counters)))
        return sum(self._segfile_counters.values(), first)

    def count_result(self, item):
        if item[1] == 1:
            self._invalid_files.add(item[0])
        if item[1:] == (0, None) and item[0] not in self._segfile_counters:
            self._skipped_files.add(item[0])
        if isinstance(item[2], SegmentFile.Counter):
            if item[0] not in self._segfile_counters:
                self._segfile_counters[item[0]] = item[2]
            else:
                self._segfile_counters[item[0]] &= item[2]
            self._skipped_files.discard(item[0])

    def __str__(self):
        out = list()
        for name in ('processed', 'invalid', 'skipped'):
            if not getattr(self, name) and name != 'processed':
                continue
            out.append('{} - {}'.format(name, getattr(self, name)))
        return 'Total files: {}. {}'.format(', '.join(out), self.segfile_counters)


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
        mimetypes.init()
        if hasattr(self.config, 'mime_types_map'):
            mimetypes.types_map.update(self.config.mime_types_map)
        if hasattr(self.config, 'module_templates'):
            logger.info('Loading external templates from %s', self.config.module_templates)
            templates.load_module(self.config.module_templates)
        for key in self.config.upload.keys():  # merge providers' settings with global ones
            if 'reprocess_invalid' not in self.config.upload[key]:
                self.config.upload[key]['reprocess_invalid'] = self.config.reprocess_invalid
            if 'force_reprocess' not in self.config.upload[key]:
                self.config.upload[key]['force_reprocess'] = self.config.force
            self.config.upload[key]['collection'] = self.config.upload[key].get(
                'collection') or self.config.segments_collection
        errors = len(self.config.clusters) - cluster.create_objects(self.config.clusters, self.config.cluster_config)
        self.pool = Pool(processes=self.config.workers)  # forking
        counter = Counter()
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
                counter.count_result(result.get())
            logger.info('%s %s', counter, timer)
            return errors + counter.invalid  # end of reprocessing paths.
        for provider in self.config.upload.copy().keys():
            if provider not in self.config.providers:
                self.config.upload.pop(provider)
        if not self.config.upload:
            logger.error('Uploading isn\'t configured. Fill in section \'upload\' in config. Set \'--providers\'.')
            return 1
        file_emitters = [FileEmitter(provider, config) for provider, config in self.config.upload.items()]
        self.wait_for_items(file_emitters)
        self.consume_queue(file_emitters)
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
                segment_file = obj.queue.get()
                self.results += [self.pool.apply_async(self.process_file, (cl_name, segment_file)) for cl_name, _ in
                                 cluster.Cluster.objects.items()]

    @staticmethod
    def process_file(cluster_name, segfile):
        """
        :return: (error_code, counter)
        """
        cl = cluster.Cluster.objects[cluster_name]
        logger = logging.getLogger('worker')
        logger.extra = {'provider': segfile.provider, 'segfile': segfile.name, 'cluster': cl.name}
        segfile.logger = logger
        try:
            cl.read_segfile_info(segfile)
        except InvalidSegmentFile as err:
            logger.error(err)
            return segfile.name, 1, None
        if segfile.processed and not segfile.invalid and not segfile.strategy.force_reprocess:
            logger.debug('The file has already been uploaded. Skipping.')
            return segfile.name, 0, None
        if segfile.invalid and not segfile.strategy.reprocess_invalid and not segfile.strategy.force_reprocess:
            logger.debug('The file is invalid. Skipping.')
            return segfile.name, 0, None
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
            return segfile.name, 1, segfile.counter
        else:
            segfile.processed = True
        finally:
            cl.save_segfile_info(segfile)
            logger.info('Finished %s. %s %s', segfile.path, segfile.counter, segfile.timer)
        return segfile.name, 1 if segfile.invalid else 0, segfile.counter
