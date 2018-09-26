# content of conftest.py
import os
import sys
import pytest
sys.modules['pymongo'] = __import__('mongomock')
from iowmongotools import cluster
import yaml

@pytest.fixture(scope="module")
# Use pymongo for real mongo. Start mongo in docker: docker run -d --rm -p 27017:27017 mongo
def local_cluster():
    local_cluster_instance = cluster.Cluster('local', {'mongos': ['localhost:27017']})

    # seed sample db config
    with open(os.path.join(os.path.dirname(__file__), 'sample_cluster.yaml'), 'r') as f:
        cluster_config = yaml.safe_load(f.read())['gce-eu']
    for key in cluster_config:
        local_cluster_instance._api.config[key].drop()
        local_cluster_instance._api.config[key].insert_many(cluster_config[key])

    return local_cluster_instance
