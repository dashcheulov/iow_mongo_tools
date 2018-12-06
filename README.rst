IOW Mongo Tools
=======================

The module has various tools for initialisation and confuguring mongo cluster, uploading and cleaning data in it.

----

There are tools (cli) on purpose:
  - mongo_check - monitoring difference between actual and declared mongo configuration
  - mongo_set - initialisation mongo cluster according to declared configuration
  - mongo_clone - cloning all data from one mongo cluster to another considering declared config
  - mongo_upload - uploading various data from files to mongo
  - mongo_cleanup - cleaning data from mongo by a condition
