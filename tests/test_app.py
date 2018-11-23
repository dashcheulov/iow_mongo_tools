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


def test_class_settings_with_nonexistent_cluster_config_file():
    defaults = {'cluster_config': ('/some/nonexistent/path/to/config.yaml',), 'property': ('sample', 'description')}
    settings_instance = app.Settings(defaults)
    assert settings_instance.property == 'sample'
    assert settings_instance.cluster_config == '/some/nonexistent/path/to/config.yaml'


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


def test_class_settings_load_cluster_config(tmpdir):
    config = tmpdir.join('tmp_cluster_config.yaml')
    config.write('''
    local:
      mongos:
      - 'localhost:27017'
    ''')
    defaults = {'cluster_config': (config.realpath(),), 'property1': ('sample', 'description')}
    settings_instance = app.Settings(defaults)
    assert settings_instance.property1 == 'sample'
    assert settings_instance.cluster_config == {'local': {'mongos': ['localhost:27017']}}


def test_class_settings_load_cluster_config_through_config(tmpdir):
    cluster_config = tmpdir.join('tmp_cluster_config.yaml')
    cluster_config.write('''
    local:
      mongos:
      - 'localhost:27019'
    ''')
    config = tmpdir.join('tmp_config.yaml')
    config.write('cluster_config: %s' % cluster_config.realpath())
    defaults = {'config_file': (config.realpath(),), 'property': ('sample', 'description')}
    settings_instance = app.Settings(defaults)
    assert settings_instance.property == 'sample'
    assert settings_instance.cluster_config == {'local': {'mongos': ['localhost:27019']}}


def test_class_settingscli_without_config(monkeypatch):
    defaults = {'property2': ('sample2', 'description2')}
    monkeypatch.setattr('iowmongotools.app.SettingsCli.load.__defaults__', (list(),))
    settings_instance = app.SettingsCli(defaults)
    assert settings_instance.__dict__ == {'property2': 'sample2'}


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
    list: [vef, efv]
    ''')
    defaults = {'config_file': (config.realpath(),), 'property1': ('to_be_erased', 'description'), 'list': ([], 'd'),
                'with-an-option': (None, 'desc'), 'option': (None, 'desc'), 'someparam': (None, 'desc'),
                }
    monkeypatch.setattr('iowmongotools.app.SettingsCli.load.__defaults__', ([
                                                                                '--list', 'one', 'two',
                                                                                '--with-an-option',
                                                                                '--no-option',
                                                                                '--someparam=val'
                                                                            ],))
    settings_instance = app.SettingsCli(defaults)
    assert settings_instance.__dict__ == {'config_file': config.realpath(),
                                          'property1': 'sample',
                                          'option': False,
                                          'someparam': 'val',
                                          'with-an-option': False,
                                          'with_an_option': True,
                                          'list': ['one', 'two']}


def test_class_settingsclicluster_load_cluster_config_through_argument(monkeypatch, tmpdir):
    cluster_config = tmpdir.join('tmp_cluster_config.yaml')
    cluster_config.write('''
    aws-va:
      mongos:
      - 'mongos1:27017'
    gce-eu:
      mongos:
      - 'mongos2:27017'
    ''')
    defaults = {'property': ('sample', 'description')}
    monkeypatch.setattr('iowmongotools.app.SettingsCli.load.__defaults__', ([
                                                                                '--cluster_config',
                                                                                str(cluster_config.realpath())
                                                                            ],))
    settings_instance = app.SettingCliCluster(defaults)
    assert settings_instance.property == 'sample'
    assert settings_instance.cluster_config['aws-va'] == {'mongos': ['mongos1:27017']}
    assert settings_instance.clusters == {'aws-va', 'gce-eu'}


def test_class_app_load_defaults_into_variable(tmpdir):
    config = tmpdir.join('tmp_config.yaml')
    config.write('''
    property: sample
    ''')

    class AppWithDefaults(app.App):
        @property
        def default_config(self):
            return {'config_file': (config.realpath(),)}

    app_instance = AppWithDefaults()
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


def test_class_command():
    sample_dict = dict()
    sample_command = app.Command(sample_dict.update, kwargs={'test': 'update_dict'}, description='updating sample_dict')
    sample_command.execute()
    assert sample_dict == {'test': 'update_dict'}
    assert str(sample_command) == 'updating sample_dict'
    with pytest.raises(TypeError) as excinfo:
        app.Command(sample_dict, {'test': 'update_dict'}, 'updating sample_dict')
    assert str(excinfo.value) == 'Command executes only callable objects'
    with pytest.raises(TypeError) as excinfo:
        app.Command(sample_dict.update, ['test', 'update_dict'], 'updating sample_dict')
    assert str(excinfo.value) == 'kwargs must be a dict'


def test_class_invoker():
    invoker = app.Invoker()
    sample_dict = dict()
    commands = list()
    for i in range(3):
        commands.append(app.Command(sample_dict.update, kwargs={'test%s' % i: 'update_dict%s' % i},
                                    description='adding test%s' % i))
    # adding a Command
    invoker.add(commands[0])
    assert len(invoker.registry) == 1
    invoker.execute()
    assert sample_dict['test0'] == 'update_dict0'
    # adding list of Commands
    invoker.add(commands[1:])
    assert len(invoker.registry) == 3
    invoker.execute(force=True)
    assert sample_dict['test2'] == 'update_dict2'
    # adding garbage instead of Command
    with pytest.raises(TypeError) as excinfo:
        invoker.add(sample_dict)
    assert str(excinfo.value) == 'Instances of \'Command\' class are allowed only'
