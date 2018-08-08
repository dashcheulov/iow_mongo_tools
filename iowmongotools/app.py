""" Contains base class for scripts. Loads settings and configures logging. Includes CLI """
import sys
import os
import logging
import logging.config
from abc import ABC, abstractmethod
from argparse import ArgumentParser
import yaml

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logger = logging.getLogger(__name__)


class Settings(object):
    """ Container with settings loaded from defaults which may be overwritten in config file or cmd-arguments"""

    def __init__(self, defaults=None):
        if isinstance(defaults, dict):
            logger.debug('Loading %s default settings', len(defaults))
            self.description = {}
            self._load_defaults(defaults)
        self.load()
        delattr(self, 'description')

    def _load_defaults(self, defaults):
        for key, content in defaults.items():  # content is tuple of value and description
            self.__dict__.update({key: content[0]})
            self.description.update({key: content[1] if len(content) == 2 else None})

    def load(self):
        self.load_config_file()

    def load_config_file(self):
        if hasattr(self, 'config_file'):
            if os.path.isfile(self.config_file):
                logger.info('Reading config file %s', self.config_file)
                with open(self.config_file, 'r') as config_file:
                    content = yaml.safe_load(config_file)
                if content:
                    self.__dict__.update(content)
            else:
                logger.warning('Cannot find config file %s', self.config_file)
                delattr(self, 'config_file')

    def __str__(self):
        return '---\n{}...'.format(yaml.safe_dump(self.__dict__, default_flow_style=False))


class SettingsCli(Settings):

    def load(self, argv=sys.argv[1:]):
        """ Loads setting from cli arguments and config_file if it is provided as cmd-argument.
        Settings are defined in the next order: defaults, config file, cmd-arguments.
        It means that defaults may be overwritten by config, which may be overwritten by cmd-args.
        """
        parser = ArgumentParser()
        parser.add_argument("--{}".format('config_file'), default=getattr(self, 'config_file') if hasattr(self, 'config_file') else None,
                            help='Path to yaml file containing settings')
        parser.parse_known_args(args=[a for a in argv if a not in ['-h', '--help']], namespace=self)
        if self.config_file:
            self.load_config_file()
        else:
            delattr(self, 'config_file')
        for key, value in self.__dict__.items():
            if not self.description.get(key):
                continue
            if value is True:
                parser.add_argument("--no-{}".format(key), dest=key, action='store_false', default=value,
                                    help=self.description.get(key))
            elif value is False:
                parser.add_argument("--{}".format(key), action='store_true', default=value,
                                    help=self.description.get(key))
            elif isinstance(value, (list, tuple, set, frozenset)):
                parser.add_argument("--{}".format(key), default=value, nargs='*', help=self.description.get(key))
            elif key not in ('config_file', 'description'):
                parser.add_argument("--{}".format(key), default=value, help=self.description.get(key))
        parser.parse_args(argv, self)


class App(ABC):
    SettingsClass = Settings

    def __init__(self):
        self.config = self.SettingsClass(self.default_config)


class AppCli(App):
    SettingsClass = SettingsCli

    def __init__(self):
        super().__init__()
        self.name = os.path.basename(sys.argv[0])  # name of script
        self.config.logging['root']['level'] = self.config.log_level.upper()
        logging.config.dictConfig(self.config.logging)
        logger.debug('%s loaded config:\n%s', self.name, self.config)

    @property
    def default_config(self):
        """ :returns Dict with default settings and its descriptions """
        return {
            'log_level': (
                'debug',
                'Level of root logger. E.g. \'info\' or \'debug\'.'
            ),
            'logging': (  # Dict passed to logging.config.dictConfig as is. Totally configures logging.
                yaml.safe_load('''
                version: 1
                disable_existing_loggers: False
                formatters:
                    console:
                        format: '%(asctime)s %(levelname)s: %(message)s'
                        datefmt: '%Y-%m-%d %H:%M:%S'
                handlers:
                    console:
                        class: logging.StreamHandler
                        formatter: console
                        stream: 'ext://sys.stdout'
                root:
                    handlers: [console]
                    level: INFO
            '''),)
        }

    @classmethod
    def entry(cls):
        return cls().run()

    @abstractmethod
    def run(self):
        raise NotImplementedError('You should implement this!')
