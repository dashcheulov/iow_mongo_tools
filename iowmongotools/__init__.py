""" Main module """
__author__ = "Denis Ashcheulov"
__version__ = "0.3.0"
__status__ = "Planning"

import logging
from multiprocessing.pool import ThreadPool
from iowmongotools import app, cluster

logger = logging.getLogger(__name__)


class MongoCheckerCli(app.AppCli):

    def run(self):
        if not hasattr(self.config, 'clusters'):
            logger.error('Please provide cluster_config.yaml. See --help.')
            return 1
        errors = len(self.config.clusters) - cluster.create_objects(self.config.clusters, self.config.cluster_config)
        pool = ThreadPool(processes=len(cluster.Cluster.objects))
        results = [pool.apply_async(_cluster.check_config) for name, _cluster in cluster.Cluster.objects.items()]
        for result in results:
            if not result.get():
                errors += 1
        return errors


class MongoSetCli(app.AppCli):

    @property
    def default_config(self):
        config = super().default_config
        config.update({
            'dry': (False, 'During dry run it won\'t be actually done anything.'),
            'force': (False, 'Certainly apply everything.')
        })
        return config

    def run(self):
        if not hasattr(self.config, 'clusters'):
            logger.error('Please provide cluster_config.yaml. See --help.')
            return 1
        errors = len(self.config.clusters) - cluster.create_objects(self.config.clusters, self.config.cluster_config)
        pool = ThreadPool(processes=len(cluster.Cluster.objects))
        results = [pool.apply_async(self.process_cluster(_cluster)) for name, _cluster in
                   cluster.Cluster.objects.items()]
        for result in results:
            if not result.get():
                errors += 1
        return errors

    def process_cluster(self, cluster):
        invoker = app.Invoker()
        actual_config = cluster.actual_config
        logger.warning('Config of cluster %s is not empty. Skipping', cluster.name)
        commands = cluster.generate_commands()
        if len(actual_config['shards']) == 0 or self.config.force:
            invoker.add(commands['add_shards'])
        else:
            logger.warning('There are already shards %s at cluster %s. Skipping adding shards', actual_config['shards'],
                           cluster.name)
        sharded_dbs = [db for db, params in actual_config['databases'].items() if params['partitioned']]
        if len(sharded_dbs) == 0 or self.config.force:
            invoker.add(commands['enable_sharding'])
        else:
            logger.warning('Sharding is enabled on %s at cluster %s. Skipping enabling sharding', sharded_dbs,
                           cluster.name)
        if actual_config['collections'] == {} or self.config.force:
            invoker.add(commands['shard_collections'])
        else:
            logger.warning('There are collections %s in cluster %s. Skipping sharding collections',
                           actual_config['collections'], cluster.name)
        if self.config.dry:
            invoker.print()
        else:
            invoker.execute()
