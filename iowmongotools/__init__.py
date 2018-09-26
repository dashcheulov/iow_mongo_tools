""" Main module """
__author__ = "Denis Ashcheulov"
__version__ = "0.2.4"
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
