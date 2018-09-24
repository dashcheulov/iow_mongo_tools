""" Manages mongo cluster """
from iowmongotools import app
import pymongo
import logging
import yaml

logger = logging.getLogger(__name__)


def create_objects(clusters, cluster_config):
    """
    :type clusters: iterable
    :param clusters: list of cluster's names to create
    :type cluster_config: dict
    :type cluster_config: configuration of clusters. Keys are names, values are configs.

    :returns amount of created objects
    """
    counter = 0
    for name in clusters:
        try:
            Cluster(name, cluster_config[name])
            counter = +1
        except KeyError as key:
            logger.error('Cannot find config for cluster %s', key)
    return counter

class Cluster(object):
    """ Represents mongo cluster """
    objects = dict()

    def __new__(cls, name, cluster_config):
        if name not in cls.objects:
            cls.objects[name] = super().__new__(cls)
        return cls.objects[name]

    def __init__(self, name, cluster_config):
        """
        :type name: str
        :param name: unique name of the cluster
        :type cluster_config: dict

        :Example:
        ..code - block:: yaml

        mongos:
        - 'mongo-gce-or-1.project.iponweb.net:27017'
        - 'mongo-gce-or-2.project.iponweb.net:27017'
        shards:
        - 'mongo-gce-or-1.project.iponweb.net:27017'
        - 'mongo-gce-or-2.project.iponweb.net:27017'
        databases:
          admin:
            partitioned: false
          project:
            partitioned: true
        """
        self._declared_config = cluster_config
        self._api = pymongo.MongoClient(['mongodb://%s' % mongos for mongos in cluster_config['mongos']], connect=False)
        self._name = name

    @property
    def actual_config(self):
        logger.debug('Reading configuration of cluster %s', self._name)
        db = self._api.config
        out = dict()
        out['mongos'] = [col['_id'] for col in db['mongos'].find()]
        out['shards'] = [col['host'] for col in db['shards'].find()]
        out['databases'] = [{col['_id']: {'partitioned': col['partitioned']}} for col in db['databases'].find()]
        out['collections'] = [{col['_id']: {'key': col['key'], 'unique': col['unique']}} for col in
                              db['collections'].find() if not col['dropped']]  # not return dropped collections
        return out

    def check_config(self):
        actual_config = self.actual_config
        if self._declared_config == actual_config:
            logger.info('Declared configuration of cluster \'%s\' is actual.', self._name)
            return True
        else:
            logger.warning(
                'Declared configuration of cluster \'%s\' is not actual.\nDeclared:\n---\n%s...\nActual:\n---\n%s...',
                self._name, yaml.safe_dump(self._declared_config, default_flow_style=False),
                yaml.safe_dump(actual_config, default_flow_style=False))
            return False
