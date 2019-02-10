Readme
======

The module has various tools for initialisation and confuguring mongo cluster, uploading and cleaning data in it.

List of tools.
--------------

There are tools (cli) on purpose:

- `mongo_check` - monitoring difference between actual and declared mongo configuration

- `mongo_set` - initialisation mongo cluster according to declared configuration

- `mongo_clone` - cloning all data from one mongo cluster to another considering declared config

- `mongo_upload` - uploading various data from files to mongo

.. - `mongo_cleanup` - cleaning data from mongo by a condition

Each tool has parameter ``--help`` with description of available arguments [1]_. Parameters may be passed as cli-arguments or declared in config.yaml. In last case config.yaml has to be passed with argument ``--config_file``, e.g. ``mongo_upload --config_file path_to/config.yaml``.

Inventory of clusters.
----------------------
Each tool works with set of mongo clusters and uses common inventory which is passed as map or path to yaml in parameter `cluster_config`. First thing you have to do is declaring right inventory.
Minimal cluster_config looks like:

.. code-block:: yaml

    cluster_config:
      <cluster_name>:
        mongos:
        - localhost:2017

For example, if it was ``cluster_config: path_to/cluster_config.yaml``, then content of ``path_to/cluster_config.yaml`` would consist from descriptions of a cluster like the next example [2]_.

.. code-block:: yaml

    gce-eu:
      mongos:
      - mongo-gce-or-1.project.net:27017
      shards:
      - mongo-gce-or-1.project.net:27019
      collections:
      - project.cookies:
          key:
            _id: hashed
          unique: false
      - project.uuidh:
          key:
            _id: hashed
          unique: false
      databases:
      - admin:
          partitioned: false
      - test:
          partitioned: false
      - project:
          partitioned: true

`mongo_check` may help to create the inventory from existent setup. Just create minimal config as described above and run `mongo_check`.

mongo_set
---------
Configures new cluster as described in the inventory. Sends following commands consistently to each mongo cluster. The next command will be sent only after positive response of mongo on previous command.

1. Disable balancer
2. Remove databases (parameter `pre_remove_dbs`) from every shard.
3. Add shards.
4. Check amount of added shards. [3]_
5. Enable sharding for databases.
6. Shard collections.

By default, a command will be sent only if appropriate configuration of cluster is empty. Shards will be added if there are no shards, collections will be sharded if there are no sharded collections, etc.

mongo_upload
------------

Configuration file.
~~~~~~~~~~~~~~~~~~~
**config.yaml** of **mongo_upload** will be described at this chapter. Other tools have simple config which may be formed with help of `<command> --help`.
Here is example of maximum filled config.

.. code-block:: yaml

    ---
    log_level: info
    clusters:
    - gce-be
    - aws-jp
    workers: 1
    providers:
    - liveramp
    segments_collection: project.cookies
    mime_types_map:
      '.log': text/tab-separated-values
    metrics:
      path: '/var/spool/metricsender/mongo_upload.txt'
      prefix: mongo_upload
      flush_interval: 60
    redis:
      host: localhost
      port: 6379
      db: 1
      password: xdX6nim7nMRc6vogrrlZGNnNvoL6i4B8
    delay_coefficient: 100
    mongo_client_settings:
      w: 0
    cluster_config: '/etc/iow-mongo-tools/cluster_config.yaml'
    module_templates: '/etc/iow-mongo-tools/templates.py'
    upload:
      liveramp:
        upsert: false
        override_filename_from_path:
          '^.*\/(.+)\.csv(?:\.gz)?': '\g<1>'
        file_type_override: text/tab-separated-values
        fixed_line_size: true
        batch_size: 1000
        write_concern:
          w: 1
        threshold_percent_invalid_lines_in_batch: 80
        process_invalid_file_to_end: true
        log_invalid_lines: true
        clusters:
          - gce-be
        delivery:
          local:
            path: /storage/liveramp
            filename: '.*\.csv(\.gz)?$'
            recursive: false
            polling_interval: 5
        input:
          text/tab-separated-values:
            - uuid: '^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}$' # uuid v4
            - segments: '^[0-9a-z_]+(?:,[0-9a-z_]+)*$' # segments separated by comma
        update_one:
          filter:
            _id: "{{uuid}}"
          update:
            $set:
              lrp_cs_us: '{{external_template}}'
              lrp_exp: '{{timestamp}}'
        templates:
          external_template:
            segment_separator: ','
            retention: 2W
        sorting:
          file_path_regexp: '^.*([0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{6})_(?:file)?part_?([0-9]+)\.csv(?:\.gz)?$'
          order:
            - path.0: asc
            - path.1: asc
            - stat.st_mtime: asc
    ...

**log_level**. Level of root logger. E.g. `info` or `debug`.

**clusters**. List of clusters from the inventory (cluster_config) which the script is going to work with.

**workers** Amount of workers. By default equals to amount of clusters from the parameter above. Unit of parallelism is segment file plus cluster. But several files of one provider cannot be uploaded in one cluster simultaneously.

**providers**. This list is just filter for items to be processed from section `upload`. More useful as cli-argument. By debault all providers from `upload` will be processed.

**segments_collection**. Full name of collection which will be updated. ``<database>.<collection>``. Metadata will be written to collection ``<database>.segment_files``

**mime_types_map**. Addition map of file extension to mime type non-standard ones.

**metrics**. If presented, the scrips will write 3 metrics: `lines_processed`, `invalid`, `uploaded` [4]_ each ``flush_interval``. The script repeatedly write values, which are collected during one flash interval, to file by ``path`` in format ``<prefix>.<provider>.<cluster>.<name> <value> <unix_timestamp>``. Every flushing, all metric counters are reset.

**redis**. Setting being passed to redis client which stores the most recent information about mongo timeouts. If this section is presented, separate daemon process will adjust intensity of uploading according to mongo timeouts. It simply counts delays which will be inserted after each batch of requests. The more timeouts, the longer delays.

**delay_coefficient** Affects the ratio between the amount of timeouts and length of delays. Is 100 by default. The higher coefficient, the longer delays.

**mongo_client_settings**. Map passed to pymongo.MongoClient() as is.

**cluster_config**. The inventory of mongo clusters. Mentioned in chapter *Inventory of clusters*.

**module_templates**. Path to python file. If presented, it will be loaded as module and external templates from it will be available.

**upload**. Section listing providers and their configs. Config of a provider consists of:

    **upsert**. If set to true, creates a new document when no document matches the query criteria. False by default.

    **override_filename_from_path**. File name is internal identificator and must be unique. This parameter allows extracting any part of absolute path of file and generate new file name from them.

    **file_type_override**. Sometimes it's possible to meet tab-separated-values inside a file with extension '.csv'. This parametes overrides guessed type of file.

    **fixed_line_size**. If it's `true` (by default), amount of columns in each line of file must be equal to amount of items in section ``input``.

    **batch_size**. File is always processed in batches. In a loop, amount of lines, given by this parameter, are read from a file, validated, transformed to mongo requests and send to mongo with bulkWrite().

    **write_concern**. Map, passed to bulkWrite(). If `write_concern is unacknowledged <https://docs.mongodb.com/manual/reference/write-concern/>`_, them matched, upserted counters and metric 'uploaded' will be always equal zero.

    **threshold_percent_invalid_lines_in_batch**. At every batch percent of invalid lines is counted. If it is above given threshold, file will be marked as invalid and logging of invalid lines will be stopped.

    **process_invalid_file_to_end**. By default, file will be processed to the end unconditionally. If this is set to `false`, processing of file will be stopped after it bacame 'invalid'. See the parameter above.

    **log_invalid_lines**. By default each line being not passed validation is logged as warning. Set the parameter to `false` in order to switch off logging of such lines.

    **clusters**. You may restrict list of clusters for particular provider.

    **delivery**. This section describes where to get files with segments. May include sub-section 'local', 's3', 'sftp', 'gs' are going to be added. May have several deliveries. E.g. if 'local' and 's3' are declared, files from both sources will be discovered and processed in order described in section `sorting`.

        **local**. Scan files on local filesystem.

            **path**. Path to scanned directory.

            **filename**. Regular expression by which files will be filtered.

            **recursive**. If `true`, files from subdirectories will be taken as well.

            **polling_interval**. During script running the directory is scanned for new files at interval defined by this parameter in seconds. Also, when new file is discovered, the script will check size of the file, wait for the interval divided by 2 and check the size again. If the size doesn't change, the file will be put in a queue to process. Otherwise, the script will consider file as being uploaded and will be waiting until uploading finishes.

    **input**. In this section there is description of input format. It consists of one of more possible types of incoming files. Content of each line is split to named columns by separator which depends on type of file. Then named values are validated by corresponding regexp. From sample config above we expect tsv file with two columns: uuid and segments. If value of any of them isn't matched to defined regexp, line will beacme `invalid`.

    **update_one**. Consists of subsections `filter` and `update` [5]_ which will be parsed and passed to mongo as `call of UpdateOne() <https://docs.mongodb.com/manual/reference/method/db.collection.updateOne>`_. Parsing assumes replacement keywords in double braces to corresponding named column from section `input` or named transformation aka `template`. Template generates string or map from input line. See details further.

    **templates**. Each `template` used in `update_one` may have config which described in this section in subsection with name of template.

    **sorting**. When several files are discovered, they will be put to processing queue in order defined in this section.

        **file_path_regexp**. Absolute path of segment file is matched and groups will be used in the next section.

        **order**. This is list of sorting rules being applied in declared order. A rule is a one-item map of a key and sorting order as value: asc or desc. As keys there are available enumerated parts of path being extracted from `file_path_regexp` and properties of function stat() such as 'st_size', 'st_atime', 'st_mtime', 'st_ctime'.

Templates.
~~~~~~~~~~
`template` is named transformation. Template receive parsed and validated line as input and return string or dict which will be used as replacement of dynamic part of updateOne() query to mongo.
A template may have own config. Once defined, parameters of it may be used in method apply(), which is applied to every line and performs transformation.
There is a couple of embedded templates. See test_templates.py for visual examples.
External templates can be loaded from python file. Here is example.

.. code-block:: python

    #!/usr/bin/env python3
    """ Transformations of a line from a segment file to a piece of json """
    from iowmongotools.templates import Template


    class SampleExternalTemplate(Template):
        def __init__(self, config):
            super().__init__()
            self.path = config.get('separator', ',')

        def apply(self, dict_line):
            return '|'.join(dict_line['segments'].split(self.separator))


    MAP = {
        'se_template': SampleExternalTemplate
    }

    if __name__ == '__main__':
        template = SampleExternalTemplate({})
        assert template.apply({'segments': '322784,159268,162274'}) == '322784|159268|162274'

Notes
+++++

.. [1] Defaults in ``<command> --help`` may be different from ones in ``<command> --config_file path_to/config.yaml --help``
.. [2] There are only sharded collections in section `collections`.
.. [3] This step appears because of that tokumx responses 'OK' immediately after adding bunch of shards despite that shards haven't been added at the moment.
.. [4] Metric `uploaded` will always be zero if write concern is unacknowledged (w: 0). See description of `write_concern`.
.. [5] Subsection `update` may consists of both ``$set`` or ``$unset``,  in this case a request will be divided to two UpdateOne() calls (one with ``$set``, other with ``$unset``) with the same filter.