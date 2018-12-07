#!/usr/bin/env python3
""" Filesystem helpers """
import os
import logging
import glob
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from multiprocessing import Process, Event, SimpleQueue

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name

NAMES_MAP = {
    'local': 'LocalFilesObserver'
}


class EventHandler(object):

    def __init__(self):
        self.items_ready = Event()
        self.queue = SimpleQueue()

    def on_modify(self, path):
        logger.warning('Size of %s is changing (%s). Probably file is being uploaded now. Waiting for it.',
                       path, os.path.getsize(path))
        self.items_ready.clear()

    def on_file_discovered(self, path):
        self.items_ready.set()

    def sort(self, files):
        return sorted(list(files))

    def dispatch(self, path, type_name):
        method_map = {
            'IN_CLOSE_WRITE': 'on_file_discovered',
            'IN_MODIFY': 'on_modify',
        }
        getattr(self, method_map[type_name])(path)


class Observer(Process, ABC):

    def __init__(self, handler):
        super().__init__()
        self.daemon = True
        if not isinstance(handler, EventHandler):
            raise TypeError("Only instances of '%s' class are allowed" % EventHandler.__name__)
        self.observable = handler
        self._files_prev = set()
        # Automatically starting
        self.start()

    @property
    @abstractmethod
    def files(self):
        """ :return set of files """
        return set()

    def get_new_files(self):
        files = self.files
        new_files = self.observable.sort(files.difference(self._files_prev))
        if not new_files:
            logger.debug('%s: No new files', self)
            return ()
        self._files_prev = files
        return new_files


class LocalFilesObserver(Observer):

    def __init__(self, handler, config):
        self.path = config['path']
        if not os.path.exists(self.path):
            raise FileNotFoundError('%s doesn\'t exist.' % self.path)
        self.filename = config.get('filename', '**')
        self.recursive = config.get('recursive', False)
        self.polling_interval = config.get('polling_interval', 5)
        super().__init__(handler)

    @property
    def files(self):
        return set(
            filename for filename in glob.glob(os.path.join(self.path, self.filename), recursive=self.recursive) if
            os.path.isfile(filename))

    def run(self):
        while True:
            self.observable.items_ready.clear()
            for path in self.get_ready_file(self.get_new_files()):
                self.observable.dispatch(path, 'IN_CLOSE_WRITE')
            self.observable.items_ready.set()
            time.sleep(self.polling_interval)

    def get_ready_file(self, new_files):
        files = OrderedDict()
        for fl in new_files:
            files[fl] = os.path.getsize(fl)
        while files:
            time.sleep(self.polling_interval / 2)
            for fl, size in files.copy().items():
                if os.path.getsize(fl) == size:
                    del (files[fl])
                    yield fl
                else:
                    files[fl] = os.path.getsize(fl)
                    self.observable.dispatch(fl, 'IN_MODIFY')
