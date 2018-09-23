# content of conftest.py
import pytest
from iowmongotools import cluster


@pytest.fixture(scope="module")
# Start mongo in docker. docker run -d --rm -p 27017:27017 mongo
def local_cluster():
    import yaml
    import pymongo
    client = pymongo.MongoClient()
    # seed sample db config
    with open('sample_cluster.yaml', 'r') as f:
        cluster_config = yaml.safe_load(f.read())['gce-eu']
    for key in cluster_config:
        client.config[key].drop()
        client.config[key].insert_many(cluster_config[key])
    client.close()

    return cluster.Cluster('local', {'mongos': ['localhost:27017']})
