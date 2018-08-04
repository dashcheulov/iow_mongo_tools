""" Main module """
__author__ = "Denis Ashcheulov"
__version__ = "0.0.2"
__status__ = "Planning"

import sys
import logging
from iowmongotools import app

logger = logging.getLogger(__name__)


def hello():
    return "Hello world!"


class MongoCheck(app.App):

    @property
    def default_config(self):
        config = super().default_config
        config['logging'][0]['formatters']['console'].update(
            {'format': '%(asctime)s %(levelname)s %(module)s: %(message)s'})
        config.update({
            'efgbvwef': ('config.json', 'wefve')
        })
        return config


class MongoCheckCli(MongoCheck, app.AppCli):

    def run(self):
        logger.debug('wefv')
        sys.exit(0)
