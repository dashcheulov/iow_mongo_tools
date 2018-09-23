""" Manages mongo cluster """
from iowmongotools import app
import pymongo
import logging
import yaml

logger = logging.getLogger(__name__)


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
        self.declared_config = cluster_config
        self._api = pymongo.MongoClient(['mongodb://%s' % mongos for mongos in cluster_config['mongos']], connect=False)
        self.name = name

    @property
    def actual_config(self):
        logger.debug('Reading configuration of cluster %s', self.name)
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
        if self.declared_config == actual_config:
            logger.info('Declared configuration of cluster \'%s\' is actual.', self.name)
            return True
        else:
            logger.warning(
                'Declared configuration of cluster \'%s\' is not actual.\nDeclared:\n---\n%s...\nActual:\n---\n%s...',
                self.name, yaml.safe_dump(self.declared_config, default_flow_style=False),
                yaml.safe_dump(actual_config, default_flow_style=False))
            return False


class MongoChecker(app.App):

    @property
    def default_config(self):
        config = super().default_config
        config.update({
            'all_clusters': (False, 'Check all clusters')
        })
        return config


class MongoCheckerCli(MongoChecker, app.AppCli):

    def run(self):
        logger.debug('wkbkbefv')
        logger.info('vjn')
        return 0
