from iowmongotools import cluster, app

sample_cluster_config = {
    'mongos': ['mongo-gce-or-1.project.iponweb.net:27017', 'mongo-gce-or-2.project.iponweb.net:27017',
               'mongo-gce-or-3.project.iponweb.net:27017'],
    'shards': ['mongo-gce-or-1.project.iponweb.net:27019', 'mongo-gce-or-2.project.iponweb.net:27019',
               'mongo-gce-or-3.project.iponweb.net:27019'],
    'databases': {'admin': {'partitioned': False}, 'db': {'partitioned': False},
                  'test': {'partitioned': False}, 'project': {'partitioned': True}},
    'collections': {'project.cookies': {'key': {'_id': 'hashed'}, 'unique': False},
                    'project.uuidh': {'key': {'_id': 'hashed'}, 'unique': False}}}


def test_cluster_instance_is_singleton(local_cluster):
    # creating instance of existing cluster must return the same object
    _client = local_cluster._api
    new_cluster_instance = cluster.Cluster('local', {'mongos': ['localhost:27017']})
    local_cluster._api = _client
    assert local_cluster is new_cluster_instance


def test_cluster_actual_config(local_cluster):
    # sample cluster's config from sample_cluster.yaml is properly read with property actual_config from mongo
    assert local_cluster.actual_config == sample_cluster_config


def test_cluster_check_config(local_cluster):
    # check_config should fail on local cluster because we declared other in sample_cluster.yaml
    assert local_cluster.check_config() is False
    local_cluster._declared_config = sample_cluster_config
    assert local_cluster.check_config() is True


def test_create_objects():
    cluster.Cluster.objects = dict()  # erase objects left from previous test
    assert 2 == cluster.create_objects(('gce-eu', 'aws-va'),
                                       {'gce-eu': sample_cluster_config, 'aws-va': {'mongos': ['l:27017']}})
    assert [v._name for k, v in cluster.Cluster.objects.items()] == ['gce-eu', 'aws-va']
    assert [v._declared_config for k, v in cluster.Cluster.objects.items()] == [sample_cluster_config,
                                                                                {'mongos': ['l:27017']}]


def test_create_objects_with_partial_config():
    cluster.Cluster.objects = dict()  # erase objects left from previous test
    assert 1 == cluster.create_objects(('gce-sc', 'aws-jp'), {'aws-jp': sample_cluster_config})
    assert [v._name for k, v in cluster.Cluster.objects.items()] == ['aws-jp']
    assert [v._declared_config for k, v in cluster.Cluster.objects.items()] == [sample_cluster_config]


def test_generate_commands():
    sample_cluster = cluster.Cluster('cluster_name', sample_cluster_config)
    actual_description, actual_kwargs = list(), list()
    for name, cmds in sample_cluster.generate_commands().items():
        for cmd in cmds:
            actual_description.append(str(cmd))
            actual_kwargs.append(cmd.kwargs)
    description_of_all_commands = [
        'adding shard mongo-gce-or-1.project.iponweb.net:27019 to cluster_name',
        'adding shard mongo-gce-or-2.project.iponweb.net:27019 to cluster_name',
        'adding shard mongo-gce-or-3.project.iponweb.net:27019 to cluster_name',
        'enabling sharding for database project',
        'enabling sharding for collection project.cookies by {\'key\': {\'_id\': \'hashed\'}, \'unique\': False}',
        'enabling sharding for collection project.uuidh by {\'key\': {\'_id\': \'hashed\'}, \'unique\': False}']
    kwargs_of_all_commands = [
        {'addshard': 'mongo-gce-or-1.project.iponweb.net:27019', 'name': 'mongo-gce-or-1'},
        {'addshard': 'mongo-gce-or-2.project.iponweb.net:27019', 'name': 'mongo-gce-or-2'},
        {'addshard': 'mongo-gce-or-3.project.iponweb.net:27019', 'name': 'mongo-gce-or-3'},
        {'enableSharding': 'project'},
        {'shardCollection': 'project.cookies', 'key': {'_id': 'hashed'}, 'unique': False},
        {'shardCollection': 'project.uuidh', 'key': {'_id': 'hashed'}, 'unique': False}]
    assert actual_description == description_of_all_commands
    assert actual_kwargs == kwargs_of_all_commands


# requires real pymongo or extending mongomock
# def test_commands():
#     config = {'mongos': ['localhost:27017'], 'shards': sample_cluster_config['shards'],
#               'databases': sample_cluster_config['databases'], 'collections': sample_cluster_config['collections']}
#     import sys
#     sys.modules['pymongo'] = __import__('pymongo')
#     import pymongo
#     real_cluster = cluster.Cluster('local', config)
#     real_cluster._api = pymongo.MongoClient(['mongodb://%s' % mongos for mongos in config['mongos']], connect=False)
#     sys.modules['pymongo'] = __import__('mongomock')
#     invoker = app.Invoker()
#     for name, cmds in real_cluster.generate_commands().items():
#         invoker.add(cmds)
#     # clean config database
#     for key in sample_cluster_config:
#         if key != 'mongos':
#             real_cluster._api.config[key].drop()
#     assert real_cluster.actual_config == {'collections': {}, 'databases': {}, 'mongos': [], 'shards': []}
#     invoker.execute()
#     assert real_cluster.actual_config == {'collections': {}, 'databases': {}, 'mongos': [], 'shards': []}
