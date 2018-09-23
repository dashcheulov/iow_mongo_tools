Changelog (iow-mongo-tools)
===========================

0.2.0 (2018-09-23)
------------------
- added entity config_cluster to Settings and SettingsCli. #IOWOPS-13114
- changed Settings.load_config() #IOWOPS-13114
- got parsed arguments with ArgumentDefaultsHelpFormatter. #IOWOPS-13114
- added singleton Cluster with tests. #IOWOPS-13114

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