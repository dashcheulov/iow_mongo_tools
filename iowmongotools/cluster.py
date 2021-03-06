""" Manages mongo cluster """
from iowmongotools import app
import logging
from time import sleep
from multiprocessing.pool import ThreadPool
import pymongo
import yaml

logger = logging.getLogger(__name__)


def create_objects(clusters, cluster_config):
    """
    :type clusters: iterable
    :param clusters: list of cluster's names to create
    :type cluster_config: dict
    :param cluster_config: configuration of clusters. Keys are names, values are configs.

    :returns amount of created objects
    """
    counter = 0
    for name in clusters:
        try:
            Cluster(name, cluster_config[name])
            counter += 1
        except KeyError as key:
            logger.error('Cannot find config for cluster %s', key)
    return counter


class Cluster(object):
    """ Represents mongo cluster """
    objects = dict()
    SEGFILE_INFO_COLLECTION = 'segment_files'

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
        mongo_client_settings = cluster_config.pop('mongo_client_settings', dict())
        self._declared_config = cluster_config
        self._api = pymongo.MongoClient(['mongodb://%s' % mongos for mongos in cluster_config['mongos']], connect=False,
                                        **mongo_client_settings)
        self.name = name
        self.uploading_delay = None

    def generate_commands(self, pre_remove_dbs=(), force=False):
        """ :returns dict of lists of commands """
        sharded_dbs = (db for db, params in self._declared_config['databases'].items() if params['partitioned'])
        commands = {
            'add_shards': [app.Command(self._api.admin.command, ('addshard', shard), {'name': shard.split('.')[0]},
                                       'adding shard %s to %s' % (shard, self.name),
                                       lambda x: x.get('ok') == 1.0) for shard in self._declared_config['shards']],
            'enable_sharding': [app.Command(self._api.admin.command, ('enableSharding', db_name),
                                            description='enabling sharding for database %s' % db_name,
                                            check_result=lambda x: x.get('ok') == 1.0) for db_name in sharded_dbs],
            'shard_collections': [app.Command(self._api.admin.command, ('shardCollection', name), params,
                                              'enabling sharding for collection %s by %s' % (name, params),
                                              lambda x: x.get('ok') == 1.0)
                                  for name, params in self._declared_config['collections'].items()],
            'pre_remove_dbs': app.Command(self.drop_databases_from_shards, (pre_remove_dbs, force),
                                          description='{} databases {} on each mongod'.format(
                                              'checking for existing' if not pre_remove_dbs else 'removing',
                                              ', '.join(pre_remove_dbs)), check_result=lambda x: x == 0),
            'waiting_shards': app.Command(self.count_shards, description='checking amount of added shards',
                                          check_result=lambda x: x == len(self._declared_config['shards']), retries=5),
            'disable_balancer': app.Command(self._api.config.settings.update,
                                            ({'_id': 'balancer'}, {'$set': {'stopped': True}}, True),
                                            description='disabling balancer', check_result=lambda x: x.get('ok') == 1.0)
        }
        return commands

    def count_shards(self):
        return len(self._api.admin.command('listShards', 1).get('shards', tuple()))

    def drop_databases_from_shards(self, dbs, force=False):
        pool = ThreadPool(processes=min(len(self._declared_config['shards']), 5))
        results = [pool.apply_async(self.remove_databases_from_shard, (shard, dbs, force)) for shard in
                   self._declared_config['shards']]
        err = 0
        for result in results:
            err += result.get()
        return err

    @staticmethod
    def remove_databases_from_shard(shard, dbs, force=False):
        dbs = frozenset(dbs)
        _api = pymongo.MongoClient('mongodb://%s' % shard, connect=False)
        try:
            actual_dbs = frozenset(_api.database_names())
            surplus_dbs = actual_dbs - (dbs | {'admin', 'local'})
            if surplus_dbs and not force:
                logger.error('There is local database %s at %s. Consider to add it to --pre_remove_dbs.',
                             ', '.join(surplus_dbs), shard)
                return 1
            for db in dbs:
                if db in actual_dbs:
                    logger.debug('Dropping database %s at %s', db, shard)
                    _api.drop_database(db)
                else:
                    logger.debug('Database %s doesn\'t exist at %s', db, shard)
                    _api.close()
        except Exception as err:
            logger.error('%s at %s', err, shard)
            return 1
        else:
            return 0
        finally:
            _api.close()

    @property
    def actual_config(self):
        logger.debug('Reading configuration of cluster %s', self.name)
        config_db = self._api.config
        out = dict({'databases': {}, 'collections': {}})
        out['mongos'] = [col['_id'] for col in config_db['mongos'].find()]
        out['shards'] = [col['host'] for col in config_db['shards'].find()]
        for db in config_db['databases'].find():
            if db['_id'] != 'db':  # exclude service database db from output
                out['databases'].update({db['_id']: {'partitioned': db['partitioned']}})
        for col in config_db['collections'].find():
            if not col['dropped']:  # not return dropped collections
                out['collections'].update({col['_id']: {'key': col['key'], 'unique': col['unique']}})
        return out

    def check_config(self):
        actual_config = self.actual_config
        for item in ('mongos', 'shards'):
            if item in actual_config and item in self._declared_config:
                actual_config[item].sort()
                self._declared_config[item].sort()
        if self._declared_config == actual_config:
            logger.info('Declared configuration of cluster \'%s\' is actual.', self.name)
            return True
        else:
            logger.warning(
                'Declared configuration of cluster \'%s\' is not actual.\nDeclared:\n---\n%s...\nActual:\n---\n%s...',
                self.name, yaml.safe_dump(self._declared_config, default_flow_style=False),
                yaml.safe_dump(actual_config, default_flow_style=False))
            return False

    def read_segfile_info(self, obj):
        collection = self._api[obj.strategy.database][self.SEGFILE_INFO_COLLECTION]
        obj.load_metadata(collection.find_one(obj.name))

    def save_segfile_info(self, obj):
        collection = self._api[obj.strategy.database][self.SEGFILE_INFO_COLLECTION]
        collection.replace_one({'_id': obj.name}, obj.dump_metadata(), upsert=True)

    def upload_segfile(self, obj):
        wc = obj.strategy.write_concern or dict()
        collection = self._api[obj.strategy.database].get_collection(obj.strategy.collection,
                                                                     write_concern=pymongo.WriteConcern(**wc))
        timer = app.Timer()
        mutable_var = [self.name, obj.provider, 0.0]
        for batch in obj.get_batch():
            if self.uploading_delay:
                if self.uploading_delay.value > 0:
                    mutable_var[2] += self.uploading_delay.value
                    sleep(self.uploading_delay.value)
                    timer.execute(self.flush_delay_to_log, (mutable_var,), 60)
            obj.shared_metrics[2] += obj.counter.count_bulk_write_result(collection.bulk_write(batch, ordered=False))

    @staticmethod
    def flush_delay_to_log(mutable_var):
        logger.warning(
            'At \'%s\' due to mongo timeouts uploading of \'%s\' has been suspended for %s seconds in the last minute.',
            mutable_var[0], mutable_var[1], round(mutable_var[2], 3))
        mutable_var[2] = 0.0
