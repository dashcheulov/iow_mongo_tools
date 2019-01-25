IOW Mongo Tools
=======================

The module has various tools for initialisation and confuguring mongo cluster, uploading and cleaning data in it.

List of tools.
--------------

There are tools (cli) on purpose:
  - `mongo_check` - monitoring difference between actual and declared mongo configuration
  - `mongo_set` - initialisation mongo cluster according to declared configuration
  - `mongo_clone` - cloning all data from one mongo cluster to another considering declared config
  - `mongo_upload` - uploading various data from files to mongo
  - `mongo_cleanup` - cleaning data from mongo by a condition

Each tool has parameter ``--help`` with description of available arguments. Parameters may be passed as cli-arguments or declared in config.yaml. In last case config.yaml has to be passed with argument ``--config_file``, e.g. ``mongo_upload --config_file path_to/config.yaml``.

  Defaults in ``<command> --help`` may be different from ones in ``<command> --config_file path_to/config.yaml --help``

Inventory of clusters.
----------------------
Each tool works with set of mongo clusters and uses common inventory which is passed as map or path to yaml in parameter `cluster_config`. First thing you have to do is declaring right inventory.
Minimal cluster_config looks like:

.. code-block:: yaml

    cluster_config:
      <cluster_name>:
        mongos:
        - localhost:2017

For example, if ``cluster_config: path_to/cluster_config.yaml``, then content of ``path_to/cluster_config.yaml`` may consists from descriptions of a cluster like the next example.

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

`mongo_check` may help to create the inventory from existent setup. Just create minimal config as described above and run `mongo_check`. There are only sharded collections in section `collections`.

Configuration file.
-------------------
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

log_level
    Level of root logger. E.g. `info` or `debug`.

clusters
    List of clusters from the inventory (cluster_config) which the script is going to work with.

workers
    Amount of workers. By default equals to amount of clusters from the parameter above. Unit of parallelism is segment file plus cluster. But several files of one provider cannot be uploaded in one cluster simultaneously.

providers
    This list is just filter for items to be processed from section `upload`. More useful as cli-argument. By debault all providers from `upload` will be processed.

segments_collection
    Full name of collection which will be updated. ``<database>.<collection>``. Metadata will be written to collection ``<database>.segment_files``

mime_types_map
    Addition map of file extension to mime type non-standard ones.

metrics
    If presented, the scrips will write 3 metrics: `lines_processed`, `invalid`, `uploaded` each ``flush_interval``. The script repeatedly write values, which are collected during one flash interval, to file by ``path`` in format ``<prefix>.<provider>.<cluster>.<name> <value> <unix_timestamp>``. Every flushing, all metric counters are reset.

mongo_client_settings
    map passed to pymongo.MongoClient() as is.

cluster_config
    The inventory of mongo clusters. Mentioned in chapter *Inventory of clusters*.

module_templates:
    Path to python file. If presented, it will be loaded as module and external templates from it will be available.

upload:
    Section listing providers and their configs. Config of a provider consists of:

    upsert:
        If set to true, creates a new document when no document matches the query criteria. False by default.

    override_filename_from_path
        File name is internal identificator and must be unique. This parameter allows extracting any part of absolute path of file and generate new file name from them.

    file_type_override
        Sometimes it's possible to meet tab-separated-values inside a file with extension '.csv'. This parametes overrides guessed type of file.

    fixed_line_size
        If it's `true` (by default), amount of columns in each line of file must be equal to amount of items in section ``input``.

    batch_size
        File is always processed in batches. In a loop, amount of lines, given by this parameter, are read from a file, validated, transformed to mongo requests and send to mongo with bulkWrite().

    write_concern
        Map, passed to bulkWrite(). See https://docs.mongodb.com/manual/reference/write-concern/ If write_concern is unacknowledged, them matched, upserted counters and metric 'uploaded' will be always equal zero.

    threshold_percent_invalid_lines_in_batch
        At every batch percent of invalid lines is counted. If it is above given threshold, file will be marked as invalid and logging of invalid lines will be stopped.

    process_invalid_file_to_end
        By default, file will be processed to the end unconditionally. If this is set to `false`, processing of file will be stopped after it bacame 'invalid'. See the parameter above.

    log_invalid_lines
        By default each line being not passed validation is logged as warning. Set the parameter to `false` in order to switch off logging of such lines.

    delivery
        This section describes where to get files with segments. May include sub-section 'local', 's3', 'sftp', 'gs' are going to be added. May have several deliveries. E.g. if declare 'local' and 's3', files from both sources will be discovered and processed in order described in section `sorting`.

        local
            Scan files on local filesystem.

            path
                Path to scanned directory.

            filename
                regular expression by which files will be filtered.

            recursive
                If `true`, files from subdirectories will be taken as well.

            polling_interval
                During script running the directory is scanned for new files at interval defined by this parameter in seconds. Also, when new file is discovered, the script will check size of the file, wait for the interval divided by 2 and check the size again. If the size doesn't change, the file will be put in a queue to process. Otherwise, the script will consider file as being uploaded and will be waiting until uploading finishes.

    input
        In this section there is description of input format. It consists of one of more possible types of incoming files. Content of each line is split to named columns by separator which depends on type of file. Then named values are validated by corresponding regexp. From sample config above we expect tsv file with two columns: uuid and segments. If value of any of them isn't matched to defined regexp, line will beacme `invalid`.

    update_one
        Consists of subsections `filter` and `update` which will be parsed and passed to mongo as call of UpdateOne(). Refer https://docs.mongodb.com/manual/reference/method/db.collection.updateOne . Parsing assumes replacement keywords in double braces to corresponding named column from section `input` or named transformation aka `template`. Template generates string or map from input line. See details further.

            Subsection `update` may consists of both ``$set`` or ``$unset``,  in this case a request will be divided to two UpdateOne() calls (one with ``$set``, other with ``$unset``) with the same filter.

    templates
        Each `template` used in `update_one` may have config which described in this section in subsection with name of template.

    sorting
        When several files are discovered, they will be put to processing queue in order defined in this section.

        file_path_regexp
            Absolute path of segment file is matched and groups will be used in the next section.

        order
            This is list of sorting rules being applied in declared order.