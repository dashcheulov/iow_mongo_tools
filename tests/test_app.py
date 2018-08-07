from iowmongotools import app
import pytest


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


def test_class_settings_with_config_file(tmpdir):
    config = tmpdir.join('tmp_config.yaml')
    config.write('''
    property1: override
    property2: null
    property3: 4.1
    property4: !!set
      sample: null
      set: null
    ''')
    defaults = {'config_file': (config.realpath(),), 'property1': ('sample', 'description')}
    settings_instance = app.Settings(defaults)
    assert settings_instance.__dict__ == {
        'config_file': config.realpath(), 'property1': 'override', 'property2': None, 'property3': 4.1,
        'property4': {'sample', 'set'}
    }


def test_class_settingscli_without_arguments(monkeypatch, tmpdir):
    config = tmpdir.join('tmp_config.yaml')
    config.write('''
    property1: sample
    ''')
    defaults = {'config_file': (config.realpath(),), 'property1': ('to_be_erased', 'description')}
    monkeypatch.setattr('iowmongotools.app.SettingsCli.load.__defaults__', (list(),))
    settings_instance = app.SettingsCli(defaults)
    assert settings_instance.__dict__ == {'config_file': config.realpath(), 'property1': 'sample'}


def test_class_settingscli_with_arguments(tmpdir, monkeypatch):
    config = tmpdir.join('tmp_config.yaml')
    config.write('''
        property1: sample
        with-an-option: false
        option: true
        someparam: to_be_erased
    ''')
    defaults = {'config_file': (config.realpath(),), 'property1': ('to_be_erased', 'description'), 'log_level': 'debug'}
    monkeypatch.setattr('iowmongotools.app.SettingsCli.load.__defaults__', ([
                                                                                '--log_level=debug',
                                                                                '--with-an-option',
                                                                                '--no-option',
                                                                                '--someparam=val'
                                                                            ],))
    settings_instance = app.SettingsCli(defaults)
    assert settings_instance.__dict__ == {'config_file': config.realpath(),
                                          'property1': 'sample',
                                          'log_level': 'debug',
                                          'option': False,
                                          'someparam': 'val',
                                          'with-an-option': False,
                                          'with_an_option': True}


def test_class_app_load_defaults_into_variable(tmpdir, monkeypatch):
    config = tmpdir.join('tmp_config.yaml')
    config.write('''
    property: sample
    ''')
    monkeypatch.setattr('iowmongotools.app.App.default_config', {'config_file': (config.realpath(), )})
    app_instance = app.App()
    assert app_instance.default_config['config_file'][0] == app_instance.config.config_file
    assert app_instance.config.property == 'sample'


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
