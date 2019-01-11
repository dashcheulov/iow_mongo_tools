""" Contains base class for scripts. Loads settings and configures logging. Includes CLI """
import sys
import os
import logging
import logging.config
from abc import ABC, abstractmethod
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import subprocess
import yaml
import re
from collections import OrderedDict

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


def human_to_seconds(string):
    """Convert internal string like 1M, 1Y3M, 3W to seconds.

    :type string: str
    :param string: Interval string like 1M, 1W, 1M3W4h2s...
        (s => seconds,
         m => minutes,
         h => hours,
         D => days,
         W => weeks,
         M => months,
         Y => Years).

    :rtype: int
    :return: The conversion in seconds of string.
    """
    interval_dict = OrderedDict([("Y", 365 * 86400),  # 1 year
                                 ("M", 30 * 86400),  # 1 month
                                 ("W", 7 * 86400),  # 1 week
                                 ("D", 86400),  # 1 day
                                 ("h", 3600),  # 1 hour
                                 ("m", 60),  # 1 minute
                                 ("s", 1)])  # 1 second
    interval_exc = "Bad interval format for {0}".format(string)

    interval_regex = re.compile(
        "^(?P<value>[0-9]+)(?P<unit>[{0}])".format("".join(interval_dict.keys())))
    seconds = 0

    while string:
        match = interval_regex.match(string)
        if match:
            value, unit = int(match.group("value")), match.group("unit")
            if int(value) and unit in interval_dict:
                seconds += value * interval_dict[unit]
                string = string[match.end():]
            else:
                raise Exception(interval_exc)
        else:
            raise Exception(interval_exc)
    return seconds


def deep_merge(a, b, path=None):
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                deep_merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


class DictConfigurator(logging.config.DictConfigurator):
    def configure_logger(self, name, config, incremental=False):
        """Configure a non-root logger from a dictionary. Extended with factory class."""
        factory_class = config.get('()', None)
        _logger_class_buf = logging.getLoggerClass()
        if factory_class:
            logging.setLoggerClass(getattr(sys.modules[__name__], factory_class))
        logger = logging.getLogger(name)
        logging.setLoggerClass(_logger_class_buf)
        self.common_logger_config(logger, config, incremental)
        propagate = config.get('propagate', None)
        if propagate is not None:
            logger.propagate = propagate


logging.config.dictConfigClass = DictConfigurator


class ConstantExtraLogger(logging.Logger):
    """Extended with constant 'extra'."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.extra = dict()

    def makeRecord(self, name, level, fn, lno, msg, args, exc_info,
                   func=None, extra=None, sinfo=None):
        rv = logging.LogRecord(name, level, fn, lno, msg, args, exc_info, func, sinfo)
        extra = extra or self.extra
        if extra is not None:
            for key in extra:
                if (key in ["message", "asctime"]) or (key in rv.__dict__):
                    raise KeyError("Attempt to overwrite %r in LogRecord" % key)
                rv.__dict__[key] = extra[key]
        return rv


class Settings(object):
    """ Container with settings loaded from defaults which may be overwritten in config file or cmd-arguments"""

    def __init__(self, defaults=None):
        if isinstance(defaults, dict):
            logger.debug('Loading %s default settings', len(defaults))
            self.description = {}
            self._load_defaults(defaults)
        self.tmp = dict()
        self.load()
        delattr(self, 'description')
        delattr(self, 'tmp')

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
        exclusions = self.extra_run(parser, argv)
        for key, value in self.__dict__.items():
            if not self.description.get(key) or key in ('config_file', 'description') + exclusions:
                continue
            if value is True:
                parser.add_argument("--no-{}".format(key), dest=key, action='store_false', default=value,
                                    help=self.description.get(key))
            elif value is False:
                parser.add_argument("--{}".format(key), action='store_true', default=value,
                                    help=self.description.get(key))
            elif isinstance(value, (list, tuple, set, frozenset)):
                parser.add_argument("--{}".format(key), default=value, nargs='*', help=self.description.get(key))
            else:
                parser.add_argument("--{}".format(key), default=value, help=self.description.get(key))
        parser.parse_args(argv, self)
        self.cleanup()
        if not getattr(self, 'config_file'):
            delattr(self, 'config_file')

    def extra_run(self, parser, argv):
        return ()

    def cleanup(self):
        pass


class SettingCliCluster(SettingsCli):

    def extra_run(self, parser, argv):
        return self.add_clusters(parser, argv)

    def cleanup(self):
        if self.tmp['cluster_config']:
            self.cluster_config = self.tmp['cluster_config']
            self.clusters = set(self.clusters)
        if not getattr(self, 'cluster_config'):
            delattr(self, 'cluster_config')

    def add_clusters(self, parser, argv):
        parser.add_argument("--{}".format('cluster_config'), default=getattr(self, 'cluster_config', None),
                            help='Path to yaml file containing description of clusters')
        parser.parse_known_args(args=[a for a in argv if a not in ['-h', '--help']], namespace=self)
        self.tmp['cluster_config'] = getattr(self, 'cluster_config')
        if os.path.isfile(str(self.tmp['cluster_config'])):
            self.tmp['cluster_config'] = self.read_config_file(self.cluster_config)
        if isinstance(self.tmp['cluster_config'], dict):
            parser.add_argument("--{}".format('clusters'), default=[c for c in self.tmp['cluster_config'].keys()],
                                help='Cluster names to be processed', nargs='*')
        return 'cluster_config',


class SettingCliUploader(SettingCliCluster):

    def extra_run(self, parser, argv):
        exclusions = self.add_clusters(parser, argv)
        parser.parse_known_args(args=[a for a in argv if a not in ['-h', '--help']], namespace=self)
        parser.add_argument("--{}".format('workers'), default=len(self.clusters) if hasattr(self, 'clusters') else 1,
                            help='Amount of workers', type=int)
        if getattr(self, 'upload'):
            parser.add_argument("--{}".format('providers'), default=[p for p in self.upload.keys()],
                                help='List of providers of segments for processing', nargs='*')
        return exclusions + ('workers', 'providers')


class Command(object):

    def __init__(self, kallable, args=tuple(), kwargs=dict(), description=str(), good_result=0):
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
        self.good_result = good_result

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
        if not isinstance(command, Command):
            raise TypeError("Instances of '%s' class are allowed only" % Command.__name__)
        self.registry.append(command)

    def execute(self, force=False):
        errors = 0
        for command in self.registry:
            if command.execute() != command.good_result:
                errors += 1
                if not force:
                    break
        return errors

    def print(self):
        for command in self.registry:
            logger.info('Would do %s', command)


class App(ABC):
    SettingsClass = Settings

    def __init__(self):
        if 'log_level' in self.default_config:
            logging_config = self.default_config['logging'][0]
            logging_config['root']['level'] = self.default_config['log_level'][0].upper()
            logging.config.dictConfig(logging_config)
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
