""" Main module """
__author__ = "Denis Ashcheulov"
__version__ = "0.0.2"
__status__ = "Planning"

import logging
from iowmongotools import app

logger = logging.getLogger(__name__)


class MongoCheck(app.App):

    @property
    def default_config(self):
        config = super().default_config
        config.update({
            'clusters': (('or', 'sc', 'eu', 'jp'), 'Set of a designation of clusters'),
            'upload_dir': ('upload', 'Directory for uploads')
        })
        return config


class MongoCheckCli(MongoCheck, app.AppCli):

    def run(self):
        logger.debug('wefv')
        return 0
