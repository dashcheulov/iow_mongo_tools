Changelog (iow-mongo-tools)
===========================

0.5.1 (2018-12-05)
------------------
- added classes Timer and Counter to module upload.
- added factory method 'log' to SegFile in order not to pickle logger object to a processes.
- now param 'cluster_config' may be path to yaml or config itself.
- handled addition in SegFile.Counter.
- took of counting results from Cluster.upload_segfile() to SegFile.Counter.count_bulk_write_result().

0.5.0 (2018-12-04)
------------------
- extended SettingCli with extra_run() and cleanup(). #IOWOPS-13673
- added SettingCliUploader and SettingCliCluster based on SettingCli. #IOWOPS-13673
- glad to introduce tool 'mongo_uploader'. #IOWOPS-13673

0.4.9 (2018-11-15)
------------------
- added mandatory check of exiting databases on mongod in 'mongo_set'. #IOWOPS-13114

0.4.8 (2018-11-15)
------------------
- check 'pre_remove_dbs' of 'mongo_set' for emptiness. #IOWOPS-13114
- 'pre_remove_dbs' is empty by default now. #IOWOPS-13114

0.4.7 (2018-11-15)
------------------
- removed debug info from Invoker.execute. #IOWOPS-13114

0.4.6 (2018-11-15)
------------------
- added proper good results to commands in Cluster.generate_commands #IOWOPS-13114
- added check for good result in Invoker.execute. #IOWOPS-13114

0.4.5 (2018-11-14)
------------------
- added parameter 'pre_remove_dbs' for 'mongo_set' #IOWOPS-13114
- added more information about errors during pre-removing databases #IOWOPS-13114
- Invoker.execute returns exit code. Added parameter force to it. #IOWOPS-13114

0.4.4 (2018-11-13)
------------------
- excluded service database 'db' from output #IOWOPS-13114

0.4.3 (2018-11-08)
------------------
- mongo_set. added step of removal database 'test' from each shard #IOWOPS-13114

0.4.2 (2018-11-07)
------------------
- fixed sensitivity of Cluster.check_config to order of shards or mongos #IOWOPS-13114

0.4.1 (2018-11-06)
------------------
- fixed ssh command of copying collection #IOWOPS-13114

0.4.0 (2018-11-06)
------------------
- deprecated `upload.run_command` in favor of `app.run_ext_command` #IOWOPS-13114
- added utility `mongo_clone` #IOWOPS-13114
- Changed type of App.config.clusters from list to set #IOWOPS-13114

0.3.2 (2018-11-02)
------------------
- fixed command 'shard_collection' #IOWOPS-13114

0.3.1 (2018-11-02)
------------------
- added args to class Command, fixed cluster's commands #IOWOPS-13114

0.3.0 (2018-11-02)
------------------
- added admin commands to cluster #IOWOPS-13114
- added endpoint mongo_set #IOWOPS-13114

0.2.4 (2018-09-26)
------------------
- mocked 'import pymongo' in tests. #IOWOPS-13114

0.2.3 (2018-09-26)
------------------
- added multithreading in MongoCheckerCli. #IOWOPS-13114
- defined default config_file. #IOWOPS-13114
- fixed counter in Cluster.create_objects(). #IOWOPS-13114

0.2.2 (2018-09-25)
------------------
- changed format of Cluster.actual_config. #IOWOPS-13114

0.2.1 (2018-09-24)
------------------
- use mongomock instead of pymongo for tests. #IOWOPS-13114
- changed version of pymongo to 3.5.1 in requirements #IOWOPS-13114
- handle case in mongo_check when cluster_config absents #IOWOPS-13114

0.2.0 (2018-09-23)
------------------
- added entity config_cluster to Settings and SettingsCli. #IOWOPS-13114
- changed Settings.load_config() #IOWOPS-13114
- got parsed arguments with ArgumentDefaultsHelpFormatter. #IOWOPS-13114
- added singleton Cluster with tests. #IOWOPS-13114
- added class MongoCheckerCli and entrypoint mongo_check. #IOWOPS-13114

0.1.9 (2018-08-15)
------------------
- improvements of classes DB and Flag. #IOWOPS-13114
- added test upload.test_segmentfile_flags_set_get. #IOWOPS-13114

0.1.8 (2018-08-09)
------------------
- changed default log level to info. #IOWOPS-13114
- don't save value to DB if it's already there. #IOWOPS-13114

0.1.7 (2018-08-08)
------------------
- add argument config_file even if it's not in defaults. #RT:515625

0.1.6 (2018-08-08)
------------------
- removed surplus argument from Uploader. #RT:515625
- set obs project in Jenkinsfile. #RT:515625

0.1.5 (2018-08-08)
------------------
- added abstractmethod to Uploader, filled in description of its defaults. #IOWOPS-13114

0.1.3 (2018-08-08)
------------------
- fixed dependencies in stdeb.cfg. #IOWOPS-13114

0.1.2 (2018-08-08)
------------------
- added dependencies to stdeb.cfg. #IOWOPS-13114

0.1.1 (2018-08-08)
------------------
- added stdeb.cfg. #IOWOPS-13114

0.1.0 (2018-08-08)
------------------
- added iowmongotools.upload. #IOWOPS-13114
- don't parse arguments without description. #IOWOPS-13114
- handle list by arguments parser. #IOWOPS-13114
- removed `config_file` from defaults of App. #IOWOPS-13114
- log warning if `config_file` absents. #IOWOPS-13114

0.0.9 (2018-08-07)
------------------
- used fixture 'tmpdir' in tests. #IOWOPS-13114
- moved 'logging' default settings from App to AppCli. #IOWOPS-13114

0.0.8 (2018-08-04)
------------------
- covered module 'app' by tests. #IOWOPS-13114

0.0.7 (2018-08-04)
------------------
- moved tests to directory `/tests`. #IOWOPS-13114

0.0.6 (2018-08-04)
------------------
- implemented module app that contains base class for scripts, loads settings and configures logging, includes CLI. #IOWOPS-13114
- moved up tests from test dir. #IOWOPS-13114

0.0.5 (2018-08-01)
------------------
- fixed test intendently broken in 0.0.2. #IOWOPS-13114

0.0.4 (2018-08-01)
------------------
- Enabled 'withPytest' in Jenkinsfile. #IOWOPS-13114
- Removed alias 'test' from setup.py. #IOWOPS-13114

0.0.3 (2018-08-01)
------------------
- Added junit xml to pytest output. #IOWOPS-13114

0.0.2 (2018-08-01)
------------------
- Integrated tests with setuptools. Intendently broke test. #IOWOPS-13114

0.0.1 (2018-08-01)
------------------
- Initialised the package #IOWOPS-13114