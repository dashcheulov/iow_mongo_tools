from iowmongotools import cluster

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
