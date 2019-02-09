Changelog
=========

0.7.3 (2019-02-10)
-------------------
- added dynamic load intensity adjustment. There are two new sections in config: 'redis' and 'delay_coefficient'.
- moved class Timer from 'uploader' to 'app'.

0.7.2 (2019-02-05)
-------------------
- added template SegmentsWithTimestamp.
- renamed 'Documents' to 'Requests to mongo' in summarise.
- upload.Strategy.set_of_used_templates() failed with not strings. fixed

0.7.1 (2019-01-25)
-------------------
- added retries and made checking result callable in app.Command
- stripping entire line only by '\n' while reading a file in mongo_uploader.
- fixed exception with empty 'cluster' in app.SettingCliUploader.cleanup()
- added steps of disabling balancer and checking amount of shards to 'mongo_set'

0.7.0 (2019-01-21)
-------------------
- upload.<provider>.update_one has appeared instead upload.<provider>.update. It contains two subsections: 'filter' and 'update'
- upload.<provider>.update_one.update may contain either '$set' or '$unset'. If it contains both of them, query will be divided to two queries with the same filter: one with '$set', other with '$unset'.

0.6.0 (2019-01-13)
-------------------
- upload.<provider>.input became ordered.
- moved Segfile.Counter and Uploader.process_file to top level in order to pickle by pickler of python 3.4

0.5.24 (2019-01-11)
-------------------
- filename is set by regexp in upload.<provider>.delivery.local.filename

0.5.23 (2019-01-11)
-------------------
- compatibility with python 3.4.

0.5.22 (2019-01-11)
-------------------
- resetting metrics' counters in Uploader.flush_metrics().

0.5.21 (2019-01-09)
-------------------
- added 'invalid' to metrics, Now 'uploaded' indicates matched + upserted documents.
- strip line in checking on header of csv.

0.5.20 (2019-01-09)
-------------------
- don't append to metrics file. Open it once at starting of 'mongo_upload'.

0.5.19 (2019-01-06)
-------------------
- fixed fall in case of lack of 'metrics' in config.

0.5.18 (2019-01-05)
-------------------
- hardened Counter with _aggregate_counters() for easy exporting metrics further.
- remove wait_for_items() in mode 'reprocess_file'.
- added method Timer.execute.
- added 'metrics' to config and method Counter.flush_metrics which is executed in main loop.
- implemented counting metrics using shared memory. #USA_DEMANDBASE-2598

0.5.17 (2018-12-28)
-------------------
- added buffer to Uploader in order to not process two sequential files from same provider at one cluster simultaneously. #USA_DEMANDBASE-2598
- added parameter 'write_concern' to config of provider.
- added checks of config parameters.
- took off main cycle fromm Uploader.run to Uploader.main

0.5.16 (2018-12-27)
-------------------
- set 'ordered=False' in bulk_write.
- added shared array to share amount of total lines between processes.
- merging 'mongo_client_settings' from global config to cluster's.
- rose test coverage to 63%.
- removed unused function upload.decompress_file.

0.5.15 (2018-12-26)
-------------------
- added section 'mongo_client_settings' to config of cluster. All options from the section are passed directly to MongoClient. #EMX-2800
- adjusted 'recursive' in local delivery.
- keeping 'lines_total' during initialisation Counter.

0.5.14 (2018-12-25)
-------------------
- stripping spaces at every field of line.
- added possibility to override name of file with parts of abs path.

0.5.13 (2018-12-24)
-------------------
- fixed fall of mongo_upload in case of lack of clusters in config.
- added possibility to override file type for provider.

0.5.12 (2018-12-23)
-------------------
- configured logging on early stage from default_config.
- made filename visible for templates.
- filtered and initiated only used templates in Strategy.
- raised InvalidSegmentFile in case of not matching to sorting pattern.
- not counting as skipped file if being processed at least at one cluster.

0.5.11 (2018-12-22)
-------------------
- fixed Counter for multi clustering and mode 'reprocess_file'.
- changed ctime to size in test_upload.test_fileemmiter_sorter.

0.5.10 (2018-12-22)
-------------------
- fixed getting segment files from queue.
- changed mtime to ctime in test_upload.test_fileemmiter_sorter

0.5.9 (2018-12-21)
------------------
- moved templates to separate module as classes.
- added posibility of extension of templates with external module. Parameter 'module_templates'.

0.5.8 (2018-12-11)
------------------
- fixed template 'timestamp'.

0.5.7 (2018-12-11)
------------------
- added template 'timestamp'. #ROME-244
- update time at every doc in template 'hash_of_segments'.

0.5.6 (2018-12-10)
------------------
- added sorting of files by fields from stats and parts of path. #EMX-2800
- fixed SettingsClass of mongo_clone, broken in 0.5.0.
- fixed that final log entry had returned nothing in 'total files' at idle running, now - 0.
- added cmd parameter 'workers' which equals to amount of clusters by default.

0.5.5 (2018-12-06)
------------------
- added parameter 'process_invalid_file_to_end' and some logic around it. #EMX-2800
- added lines to total Counter.

0.5.4 (2018-12-05)
------------------
- fixed working of param '--force' of 'mongo_clone'.

0.5.3 (2018-12-05)
------------------
- renamed section 'output' to 'update' in config.

0.5.2 (2018-12-05)
------------------
- added parameter 'log_invalid_lines' to config, 'true' by default.

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