Changelog (iow-mongo-tools)
===========================

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