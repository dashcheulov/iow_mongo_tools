""" Main module """
__author__ = "Denis Ashcheulov"
__version__ = "0.5.11"
__status__ = "Alpha"

import logging
from multiprocessing.pool import ThreadPool
from iowmongotools import app, cluster, upload

logger = logging.getLogger(__name__)


class MongoCheckerCli(app.AppCli):
    SettingsClass = app.SettingCliCluster

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
    SettingsClass = app.SettingCliCluster

    @property
    def default_config(self):
        config = super().default_config
        config.update({
            'dry': (False, 'During dry run it won\'t be actually done anything.'),
            'force': (False, 'Certainly apply everything.'),
            'pre_remove_dbs': ([], 'Databases will be removed from each mongod beforehand.')
        })
        return config

    def run(self):
        if not hasattr(self.config, 'clusters'):
            logger.error('Please provide cluster_config.yaml. See --help.')
            return 1
        errors = len(self.config.clusters) - cluster.create_objects(self.config.clusters, self.config.cluster_config)
        pool = ThreadPool(processes=len(cluster.Cluster.objects))
        results = [pool.apply_async(self.process_cluster, (_cluster,)) for name, _cluster in
                   cluster.Cluster.objects.items()]
        for result in results:
            if result.get() != 0:
                errors += 1
        return errors

    def process_cluster(self, cluster):
        invoker = app.Invoker()
        actual_config = cluster.actual_config
        commands = cluster.generate_commands(self.config.pre_remove_dbs, self.config.force)
        if len(actual_config['shards']) == 0 or self.config.force:
            invoker.add(commands['pre_remove_dbs'])
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
                           [name for name, p in actual_config['collections'].items()], cluster.name)
        if self.config.dry:
            invoker.print()
        else:
            return invoker.execute(self.config.force)
        return 0


class MongoCloneCli(app.AppCli):
    SettingsClass = app.SettingCliCluster

    @property
    def default_config(self):
        config = super().default_config
        config.update({
            'dry': (False, 'During dry run actions will just be printed.'),
            'force': (False, 'Copy collection even it doesn\'t exist on destination. It will be created unsharded.'),
            'upsert': (True, 'Disable upserts.'),
            'src': ('', 'Name of source cluster'),
            'dst': ('', 'Name of destination cluster')
        })
        return config

    def run(self):
        if {self.config.src, self.config.dst} - self.config.clusters:
            logger.error('Both source and destination clusters must be described in cluster_config.yaml. See --help.')
            return 1
        logger.debug('Taking actual list of shards and sharded collections of cluster %s.', self.config.src)
        src_config = cluster.Cluster(self.config.src, self.config.cluster_config[self.config.src]).actual_config
        dst_config = cluster.Cluster(self.config.dst, self.config.cluster_config[self.config.dst]).actual_config
        if src_config['collections'] != dst_config['collections'] and not self.config.force:
            logger.error(
                'Source collections: %s\nDestination collections: %s\nBoth source and destination clusters are expected to have the same list of collections. Please use parameter --force or utility \'mongo_set\' to configure cluster.',
                [name for name, params in src_config['collections'].items()],
                [name for name, params in dst_config['collections'].items()])
            return 1
        logger.debug('Taking the fist mongos from config of cluster %s', self.config.dst)
        mongos, dst_port = self.config.cluster_config[self.config.dst]['mongos'][0].split(':')
        invoker = app.Invoker()
        for collection, params in src_config['collections'].items():
            database, collection = collection.split('.')
            for shard in src_config['shards']:
                shard, src_port = shard.split(':')
                invoker.add(app.Command(app.run_ext_command,
                                        (('ssh', '-o UserKnownHostsFile=/dev/null', '-o StrictHostKeyChecking=no', '-A',
                                          shard,
                                          'mongoexport -h {0} --port {1} -d {2} -c {3} | gzip -c | ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -A {4} "cat - | gunzip -c | mongoimport {5} -d {2} -c {3} -h 127.0.0.1 --port {6}"'.format(
                                              shard, src_port, database, collection, mongos,
                                              '--upsert' if self.config.upsert else '', dst_port),),),
                                        description='copy collection \'%s\' of db \'%s\' from %s to %s' % (
                                            collection, database, shard, mongos)))
        if self.config.dry:
            invoker.print()
        else:
            return invoker.execute(self.config.force)
        return 0


class MongoUploadCli(upload.Uploader, app.AppCli):
    SettingsClass = app.SettingCliUploader
