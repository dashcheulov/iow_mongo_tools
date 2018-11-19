#!/usr/bin/env python3
""" Filesystem helpers """
import os
import logging
import glob
import time
from multiprocessing import Process, Event, SimpleQueue

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class EventHandler(object):

    def __init__(self):
        self.items_ready = Event()
        self.queue = SimpleQueue()

    def on_modify(self, source, path):
        logger.warning('Size of %s is changing (%s). Probably file is being uploaded now. Waiting for it.',
                       path, os.path.getsize(path))
        self.items_ready.clear()

    def on_file_discovered(self, source, path):
        self.items_ready.set()

    def dispatch(self, source, path, type_name):
        method_map = {
            'IN_CLOSE_WRITE': 'on_file_discovered',
            'IN_MODIFY': 'on_modify',
        }
        getattr(self, method_map[type_name])(source, path)


class Observer(Process):

    def __init__(self, handler):
        super().__init__()
        self.daemon = True
        if not isinstance(handler, EventHandler):
            raise TypeError("Only instances of '%s' class are allowed" % EventHandler.__name__)
        self.observable = handler
        # Automatically start thread
        self.start()


class LocalFilesObserver(Observer):

    def __init__(self, handler, path, recursive=False, polling_interval=5):
        self.path = path
        self.recursive = recursive
        self._files_prev = set()
        self.polling_interval = polling_interval
        super().__init__(handler)

    @property
    def files(self):
        return set(filename for filename in glob.glob(os.path.join(self.path, '**'), recursive=self.recursive) if
                   os.path.isfile(filename))

    def get_new_file(self, check_for_changing=True):
        files = self.files
        new_files = files.difference(self._files_prev)
        if not new_files:
            logger.debug('No new files')
            return ()
        self._files_prev = files
        if not check_for_changing:
            for new_file in new_files:
                yield new_file
        else:
            files = dict()
            for fl in new_files:
                files[fl] = os.path.getsize(fl)
            while files:
                time.sleep(self.polling_interval / 2)
                for fl, size in files.copy().items():
                    if os.path.getsize(fl) == size:
                        files.pop(fl)
                        yield fl
                    else:
                        files[fl] = os.path.getsize(fl)
                        self.observable.dispatch(self.__class__.__name__, fl, 'IN_MODIFY')

    def run(self):
        logger.debug('Starting watcher of local filesystem.')
        while True:
            self.observable.items_ready.clear()
            for path in self.get_new_file():
                self.observable.dispatch(self.__class__.__name__, path, 'IN_CLOSE_WRITE')
            self.observable.items_ready.set()
            time.sleep(self.polling_interval)
