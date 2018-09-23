#!/usr/bin/env python3
# pylint: disable=line-too-long
""" Imports segments to mongo """
import os
import logging
import glob
import subprocess
import gzip
import shutil
import threading
import yaml
from copy import copy
from abc import abstractmethod
from iowmongotools import app

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def run_command(args, cluster):
    """ Runs external command
    :returns exit code
    """
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
        raise (ValueError, "%s unknown suffix -- ignored" % src)
    dst = os.path.join(dst_dir, os.path.basename(src.rsplit('.gz', 1)[0]))
    logger.info("Decompressing %s to %s", src, dst)
    with gzip.open(src, 'rb') as f_in:
        with open(dst, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    logger.debug("%s decompressed", src)
    return dst


class DB(object):
    """ Operates with persistent storage of metadata """
    path = None
    __data = {}
    _flushed = True

    @classmethod
    def load(cls, path=None):
        """ Reads db from file and updates to property __data """
        if not cls.path:
            if not path:
                raise NameError('Please define DB.path')
            cls.path = path
        logger.debug('Reading metadata from %s', cls.path)
        if not os.path.isfile(cls.path):
            cls.__data = {}
            return
        with open(cls.path, 'r') as db_file:
            cls.__data.update(yaml.safe_load(db_file) or {})

    @classmethod
    def flush(cls):
        """ Writes content of property __data to file only if __data is modified """
        if not cls._flushed:
            logger.debug('Writting metadata to %s', cls.path)
            with open(cls.path, 'w') as outfile:
                yaml.safe_dump(cls.__data, outfile, default_flow_style=False)
            cls._flushed = True

    def __init__(self, name, initial=None):
        self.name = name
        if initial and not isinstance(initial, dict):
            logger.warning('Wrong value %s. Ignoring.', initial)
        if name not in self.__data:
            self.__data.update({name: initial or {}})

    def update(self, item, val):
        """ Update value of item. """
        self.__data[self.name].update({item: val})
        DB._flushed = False

    def get(self, item):
        """ :return item from dict _data """
        return self.__data[self.name].get(item)


class SegmentFile(object):  # pylint: disable=too-few-public-methods
    """ Represents file containing segments """

    def __init__(self, path, config, processor):
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
        self.config = config
        self.processor = processor
        self.name = os.path.basename(self.path).split('.')[0]
        Flags.db = DB(self.name)
        self.flags = Flags()
        self.tmp_file = self.path
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


class TmpSegmentFile(object):  # pylint: disable=too-few-public-methods
    """ Context for creating temporary processed file in inprog directory
    :type obj: SegmentFile

    :Example:
    .. code-block:: python

    segfile = SegmentFile(filename)
    with TmpSegmentFile(segfile):
        segfile.process(cluster)
    """

    def __init__(self, obj):
        self._obj = obj

    def __enter__(self):
        if not os.path.isdir(self._obj.config.inprog_dir):
            logger.info('%s doesn\'t exist. Creating it.', self._obj.config.inprog_dir)
            os.mkdir(self._obj.config.inprog_dir)
        try:
            self._obj.tmp_file = decompress_file(self._obj.path, self._obj.config.inprog_dir)
        except ValueError:
            logger.info('Copying %s to %s', self._obj.name, self._obj.config.inprog_dir)
            self._obj.tmp_file = shutil.copy(self._obj.path,
                                             self._obj.config.inprog_dir)  # pylint: disable=assignment-from-no-return

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._obj.err_count:
            if not os.path.isdir(self._obj.config.invalid_dir):
                logger.info('%s doesn\'t exist. Creating it.', self._obj.config.invalid_dir)
                os.mkdir(self._obj.config.invalid_dir)
            logger.info('Moving invalid tmp file %s to %s', self._obj.name, self._obj.config.invalid_dir)
            try:
                shutil.move(self._obj.tmp_file, self._obj.config.invalid_dir)
            except shutil.Error:
                logger.warning('There is %s in %s already', self._obj.name, self._obj.config.invalid_dir)
        else:
            logger.info('Removing valid tmp file %s', self._obj.tmp_file)
            os.remove(self._obj.tmp_file)


class Uploader(app.App):
    def __init__(self):
        super().__init__()
        DB.load(self.config.db_path)

    @property
    def default_config(self):
        config = super().default_config
        config.update({
            'clusters': (('or', 'sc', 'eu', 'jp'), 'Set of a designation of clusters.'),
            'upload_dir': ('upload', 'Directory with incoming files'),
            'inprog_dir': ('inprog_dir', 'Temporary directory with files being processed.'),
            'invalid_dir': ('invalid_dir',),
            'db_path': ('db.yaml', 'Path to file containing state of uploading.'),
            'reprocess_invalid': (False, 'Whether reprocess files were not uploaded previously'),
        })
        return config

    def run(self):
        files = [SegmentFile(filename, self.config, self.processor) for filename in
                 glob.glob(os.path.join(self.config.upload_dir, '*')) if
                 os.path.isfile(filename)]
        err_count = 0
        clusters = frozenset(self.config.clusters)
        for segfile in sorted(files, reverse=True):
            if not (self.config.reprocess_invalid and segfile.flags.invalid) and clusters == (
                    segfile.flags.processed | segfile.flags.invalid):
                continue
            threads = []
            with TmpSegmentFile(segfile):
                for cluster in clusters:
                    if (cluster in segfile.flags.processed) or (
                            not self.config.reprocess_invalid and (cluster in segfile.flags.invalid)):
                        continue
                    threads.append(threading.Thread(target=segfile.process, kwargs={'cluster': cluster}))
                    threads[-1].start()
                for thread in threads:
                    thread.join()
            err_count += segfile.err_count
        return err_count

    @abstractmethod
    def processor(self, filename, cluster, config):
        raise NotImplementedError('You should implement this')
