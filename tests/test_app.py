from iowmongotools import app
import pytest
import os


class TmpConfig(object):
    def __init__(self, path, content):
        self.path = path
        self.content = content

    def __enter__(self):
        with open(self.path, 'w') as outfile:
            outfile.write(self.content)

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.remove(self.path)


def test_class_settings_with_only_defaults():
    defaults = {
        'property1': ('sample',), 'property2': (2, 'description'), 'property3': (None,),
        'property4': (['sample', 'list'],)
    }
    settings_instance = app.Settings(defaults)
    assert settings_instance.property1 == 'sample'
    assert settings_instance.property2 == 2
    assert settings_instance.property3 is None
    assert settings_instance.property4 == ['sample', 'list']
    assert settings_instance.__dict__ == {
        'property1': 'sample', 'property2': 2, 'property3': None, 'property4': ['sample', 'list']
    }


def test_class_settings_with_nonexistent_config_file():
    defaults = {'config_file': ('/some/nonexistent/path/to/config.yaml',), 'property': ('sample', 'description')}
    settings_instance = app.Settings(defaults)
    assert settings_instance.property == 'sample'
    assert hasattr(settings_instance, 'config_file') is False


def test_class_settings_with_config_file():
    defaults = {'config_file': ('tmp_config.yaml',), 'property1': ('sample', 'description')}
    content_of_config = '''
    property1: override
    property2: null
    property3: 4.1
    property4: !!set
      sample: null
      set: null
    '''
    with TmpConfig('tmp_config.yaml', content_of_config):
        settings_instance = app.Settings(defaults)
        assert settings_instance.__dict__ == {
            'config_file': 'tmp_config.yaml', 'property1': 'override', 'property2': None, 'property3': 4.1,
            'property4': {'sample', 'set'}
        }


def test_class_settingscli_without_arguments(monkeypatch):
    defaults = {'config_file': ('tmp_config.yaml',), 'property1': ('to_be_erased', 'description')}
    content_of_config = '''
    property1: sample
    '''
    with TmpConfig('tmp_config.yaml', content_of_config):
        monkeypatch.setattr('iowmongotools.app.SettingsCli.load.__defaults__', (list(),))
        settings_instance = app.SettingsCli(defaults)
        assert settings_instance.__dict__ == {'config_file': 'tmp_config.yaml', 'property1': 'sample'}


def test_class_settingscli_with_arguments(monkeypatch):
    defaults = {'config_file': ('tmp_config.yaml',), 'property1': ('to_be_erased', 'description'), 'log_level': 'debug'}
    content_of_config = '''
        property1: sample
        with-an-option: false
        option: true
        someparam: to_be_erased
    '''
    with TmpConfig('tmp_config.yaml', content_of_config):
        monkeypatch.setattr('iowmongotools.app.SettingsCli.load.__defaults__', ([
                                                                                    '--log_level=debug',
                                                                                    '--with-an-option',
                                                                                    '--no-option',
                                                                                    '--someparam=val'
                                                                                ],))
        settings_instance = app.SettingsCli(defaults)
        assert settings_instance.__dict__ == {'config_file': 'tmp_config.yaml',
                                              'property1': 'sample',
                                              'log_level': 'debug',
                                              'option': False,
                                              'someparam': 'val',
                                              'with-an-option': False,
                                              'with_an_option': True}


def test_class_app_load_defaults_into_variable():
    app_instance = app.App()
    assert app_instance.default_config['log_level'][0] == app_instance.config.log_level
    assert app_instance.default_config['logging'][0]['formatters'] == app_instance.config.logging['formatters']


def test_class_appcli_cannot_inited_without_abstract_method():
    with pytest.raises(TypeError) as excinfo:
        app.AppCli()
    assert 'abstract class AppCli with abstract methods' in str(excinfo.value)


def test_class_appcli_init_logging(monkeypatch):
    class AppCliWithRun(app.AppCli):
        def run(self):
            pass

    monkeypatch.setattr('iowmongotools.app.SettingsCli.load.__defaults__', (('--log_level=warning',),))
    appcli_instance = AppCliWithRun()
    assert app.logging.root.level == 30  # matches to warning level
    assert appcli_instance.config.logging['formatters']['console']['format'] == \
           app.logging.root.handlers[0].formatter._fmt
