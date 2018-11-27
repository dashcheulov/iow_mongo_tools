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
from copy import copy
from multiprocessing import Pool
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


class SegmentFile(object):  # pylint: disable=too-few-public-methods
    """ Represents file containing segments """

    def __init__(self, path, provider, config):
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

        self.path = path
        self.provider = provider
        self.name = os.path.basename(self.path).split('.')[0]
        # Flags.db = FileDB(self.name)
        # self.flags = Flags()
        self.tmp_file = self.path
        self.config = config
        self.err_count = 0

    def __gt__(self, other):
        return os.stat(self.path).st_mtime > os.stat(other.path).st_mtime

    def process(self, cluster):
        """ Method processing single segments file with method processor """
        try:
            exitcode = self.processor(self.tmp_file, cluster, self.config)
        except Exception as err:  # pylint: disable=broad-except
            logger.exception(err)
            exitcode = 1
            self.err_count += exitcode
        if exitcode == 0:
            logger.info('%s has been uploaded to cluster %s', self.name, cluster)
            self.flags.processed |= {cluster}
            self.flags.invalid -= {cluster}
        else:
            logger.error('%s has not been uploaded to cluster %s', self.name, cluster)
            self.flags.invalid |= {cluster}
            self.err_count += 1


class Strategy(object):

    def __init__(self, config):
        if 'input' not in config or 'output' not in config:
            raise AttributeError('Uploading strategy must have both sections \'input\' and \'output\'')
        tsv_file_schema = config['input'].get('tsv_file', dict())
        self.titles = list()
        self.patterns = list()
        for title, pattern in tsv_file_schema.items():
            self.titles.append(title)
            self.patterns.append(re.compile(pattern))
        self.template_params = self.prepare_template_params(config['input'].get('templates', dict()))

    @staticmethod
    def prepare_template_params(config):
        if 'hash_of_segments' not in config:
            config['hash_of_segments'] = dict()
        config['hash_of_segments']['expiration_ts'] = int(
            time.time() + app.human_to_seconds(config['hash_of_segments'].pop('retention', '30D')))
        if 'segment_separator' not in config['hash_of_segments']:
            config['hash_of_segments']['segment_separator'] = ','
        return config

    def get_hash_of_segments(self, segment_string):
        config = self.template_params['hash_of_segments']
        output = dict()
        for segment in segment_string.split(config['segment_separator']):
            output[segment] = config['expiration_ts']
        return output

    def get_setter(self, line):
        line = line.split('\t')
        dict_line = dict()
        for index in range(len(self.titles)):
            self.patterns[index].match(line[index])
            dict_line[self.titles[index]] = line[index]


class FileEmitter(fs.EventHandler):

    def __init__(self, provider, upload_config):
        super().__init__()
        self.provider = provider
        self.delivery_config = upload_config.pop('delivery')
        self.config = upload_config
        self.start_observers()

    def on_file_discovered(self, path):
        logger.debug('%s is discovered. Put in queue', path)
        self.queue.put(SegmentFile(path, self.provider, self.config))
        self.items_ready.set()

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
            raise ValueError("There is no known delivery for %s. Review config." % self.provider)


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
        import random
        time.sleep(random.uniform(0.1, 1))
        logger.info('%s %s of size %s from %s', cl.name, segfile.name, os.path.getsize(segfile.path), segfile.provider)
        return 1
