""" Contains base class for scripts. Loads settings and configures logging. Includes CLI """
import sys
import os
import logging
import logging.config
from abc import ABC, abstractmethod
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import subprocess
import yaml

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logger = logging.getLogger(__name__)


def run_ext_command(args, title=''):
    """ Runs external command
    :returns exit code
    """
    logger.info("Executing command: %s", " ".join(args))
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1,
                            universal_newlines=True)
    prefix = '{}{}'.format(title, '> ' if title else '')
    with proc.stdout:
        for line in proc.stdout:
            logger.info('%s%s', prefix, line.strip())
    return proc.wait()


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
        if hasattr(self, 'config_file'):
            config = self.read_config_file(self.config_file)
            if config:
                self.__dict__.update(config)
            else:
                delattr(self, 'config_file')
        if hasattr(self, 'cluster_config'):
            self.cluster_config = self.read_config_file(self.cluster_config) or self.cluster_config

    @staticmethod
    def read_config_file(filepath):
        if filepath:
            if os.path.isfile(filepath):
                logger.info('Reading config file %s', filepath)
                with open(filepath, 'r') as config_file:
                    content = yaml.safe_load(config_file)
                return content
            else:
                logger.warning('Cannot find config file %s', filepath)

    def __str__(self):
        return '---\n{}...'.format(yaml.safe_dump(self.__dict__, default_flow_style=False))


class SettingsCli(Settings):

    def load(self, argv=sys.argv[1:]):
        """ Loads setting from cli arguments and config_file if it is provided as cmd-argument.
        Settings are defined in the next order: defaults, config file, cmd-arguments.
        It means that defaults may be overwritten by config, which may be overwritten by cmd-args.
        """
        parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
        parser.add_argument("--{}".format('config_file'), default=getattr(self, 'config_file', None),
                            help='Path to yaml file containing settings')
        parser.parse_known_args(args=[a for a in argv if a not in ['-h', '--help']], namespace=self)
        config = self.read_config_file(self.config_file)
        if config:
            self.__dict__.update(config)
        parser.add_argument("--{}".format('cluster_config'), default=getattr(self, 'cluster_config', None),
                            help='Path to yaml file containing description of clusters')
        parser.parse_known_args(args=[a for a in argv if a not in ['-h', '--help']], namespace=self)
        cluster_config = self.read_config_file(self.cluster_config)
        if cluster_config:
            parser.add_argument("--{}".format('clusters'), default=[c for c in cluster_config.keys()],
                                help='Cluster names to be processed', nargs='*')
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
            elif key not in ('config_file', 'description', 'cluster_config'):
                parser.add_argument("--{}".format(key), default=value, help=self.description.get(key))
        parser.parse_args(argv, self)
        if cluster_config:
            self.cluster_config = cluster_config
            self.clusters = set(self.clusters)
        for attr in ('config_file', 'cluster_config'):
            if not getattr(self, attr):
                delattr(self, attr)


class Command(object):

    def __init__(self, kallable, args=tuple(), kwargs=dict(), description=str()):
        if not hasattr(kallable, '__call__'):
            raise TypeError("%s executes only callable objects" % Command.__name__)
        if not isinstance(args, (tuple, list)):
            raise TypeError('args must be a tuple or list')
        if not isinstance(kwargs, dict):
            raise TypeError('kwargs must be a dict')
        self.func = kallable
        self.args = args
        self.kwargs = kwargs
        self.description = description

    def execute(self):
        logger.info(self)
        return self.func(*self.args, **self.kwargs)

    def __str__(self):
        return self.description


class Invoker(object):

    def __init__(self):
        self.registry = []

    def add(self, command):
        if isinstance(command, (list, tuple)):
            for cmd in command:
                self.add(cmd)
            return
        if not issubclass(command.__class__, Command):
            raise TypeError("Instanses of '%s' class are allowed only" % Command.__name__)
        self.registry.append(command)

    def execute(self):
        errors = 0
        for command in self.registry:
            result = command.execute()
            if result is False or result != 0:
                errors += 1
        return errors

    def print(self):
        for command in self.registry:
            logger.info('Would do %s', command)


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
            'config_file': ('/etc/iow-mongo-tools/config.yaml', 'Path to yaml file containing settings'),
            'log_level': (
                'info',
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
