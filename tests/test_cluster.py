from iowmongotools import cluster


def test_cluster_instance_is_singleton(local_cluster):
    # creating instance of existing cluster must return the same object
    new_cluster_instance = cluster.Cluster('local', {'mongos': ['localhost:27017']})
    assert local_cluster is new_cluster_instance


def test_cluster_actual_config(local_cluster):
    # sample cluster's config from sample_cluster.yaml is properly read with property actual_config from mongo
    assert local_cluster.actual_config == {
        'mongos': ['mongo-gce-or-1.project.iponweb.net:27017', 'mongo-gce-or-2.project.iponweb.net:27017',
                   'mongo-gce-or-3.project.iponweb.net:27017'],
        'shards': ['mongo-gce-or-1.project.iponweb.net:27019', 'mongo-gce-or-2.project.iponweb.net:27019',
                   'mongo-gce-or-3.project.iponweb.net:27019'],
        'databases': [{'admin': {'partitioned': False}}, {'db': {'partitioned': False}},
                      {'test': {'partitioned': False}}, {'project': {'partitioned': True}}],
        'collections': [{'project.cookies': {'key': {'_id': 'hashed'}, 'unique': False}},
                        {'project.uuidh': {'key': {'_id': 'hashed'}, 'unique': False}}]}


def test_cluster_check_config(local_cluster):
    # check_config should fail on local cluster because we declared other in sample_cluster.yaml
    assert local_cluster.check_config() is False
