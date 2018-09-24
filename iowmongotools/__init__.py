""" Main module """
__author__ = "Denis Ashcheulov"
__version__ = "0.2.0"
__status__ = "Planning"

import logging
from iowmongotools import app, cluster

logger = logging.getLogger(__name__)


class MongoCheckerCli(app.AppCli):

    def run(self):
        errors = len(self.config.clusters) - cluster.create_objects(self.config.clusters, self.config.cluster_config)
        for name, _cluster in cluster.Cluster.objects.items():
            if not _cluster.check_config():
                errors += 1
        return errors
