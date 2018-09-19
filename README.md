# Kinetica Spark Connector

This project contains the **6.2** version of the **Kinetica Spark Connector** for Spark 1.6.3

TBD - This guide exists on-line at:  [Kinetica Spark Connector Guide](http://www.kinetica.com/docs/6.2/connectors/spark_guide.html)

More information can be found at:  [Kinetica Documentation](http://www.kinetica.com/docs/6.2/index.html)

-----

# Kinetica Spark Connector Guide

The following guide provides step by step instructions to get started using
*Spark* with *Kinetica*.  The *Spark Connector* provides easy integration of
*Spark v1.6.3* with *Kinetica* via the *Spark Data Source API*.

There are two ways in which this connector can interface with *Kinetica*:

1. as a configurable [data loader](#spark-data-loader), via the command line,
   which can load data into *Kinetica* via *Spark*
2. as an interactive
   [data ingest processor](#spark-ingest-processor),
   programmatically, via the *Kinetica Spark API*, which can ingest data into
   *Kinetica* from *Spark* 
3. as an interactive
   [streaming data processor](#spark-streaming-processor),
   programmatically, via the *Kinetica Spark API*, which can stream data inserts
   against *Kinetica* into *Spark*

Source code for the connector can be found at:

* <https://github.com/kineticadb/kinetica-connector-spark>

## Contents

* [Build & Install](#build--install)
* [Usage](#usage)
* [Spark Data Loader](#spark-data-loader)
* [Spark Ingest Processor](#spark-ingest-processor)
* [Property Reference](#property-reference)

## Build & Install

The connector JAR can be built with *Maven* as follows:

    $ git clone https://github.com/kineticadb/kinetica-connector-spark.git -b release/v6.2.0_SPARK163 --single-branch
    $ cd kinetica-connector-spark
    $ mvn clean package

**NOTE:**  Compilation requires Java 1.8. Ensure that ``JAVA_HOME`` is set
appropriately.

This sequence produces the connector JAR, which will be made availble to the
*Spark* cluster upon submitting the *Spark* job.  It can be found under the
``target`` directory:

    target/kinetica-spark163-6.2.1-jar-with-dependencies.jar

It will also produce a testing JAR under the same directory, which will be
referenced later in this guide for use in testing the *Spark* connector:

    target/kinetica-spark163-6.2.1-tests.jar


## Usage

To run the *Data Loader* on a *Spark* cluster, run the following command; be
sure to provide appropriate values for ``<SparkMasterHostName/IP>``,
``<SparkMasterPort>``, & ``<PropertiesFile>``:

    $ spark-submit \
       --class com.kinetica.spark.SparkKineticaDriver \
       --master "spark://<SparkMasterHostName/IP>:<SparkMasterPort>" \
       kinetica-spark163-6.2.1-jar-with-dependencies.jar <PropertiesFile>


To launch the *Ingest Processor* through the *Spark* shell, run:

    $ spark-shell --jars kinetica-spark163-6.2.1-jar-with-dependencies.jar


To run the *Ingest Processor* through the *PySpark* shell:

    $ pyspark --jars kinetica-spark163-6.2.1-jar-with-dependencies.jar


## Spark Data Loader

The *Spark Data Loader* fetches data from a SQL ``SELECT`` statement or data
file and inserts the results into a *Kinetica* table.

Features include:

* Supports input from SQL, AVRO, and common *Spark* formats
* Configuration file driven
* No coding, other than the input SQL statement
* *Spark* types are automatically mapped to *Kinetica* types
* Tables can be automatically created or truncated
* Schemas can be version controlled with [Template Tables](#template-tables)
* Differences between *Spark DataFrames* and *Kinetica* tables are automatically
  reconciled


### Property Specification

Properties can be set in one of the following locations:

* Top-level file: properties file passed in on the command line
* Included file: properties file included from the top-level properties file
* Command line: command line argument that overrides values in any of the other
  properties files

For the data source, either a file containing a SQL-compliant query or a data
file/path must be specified:

* *Query File*: The contained SQL statement is executed and data is retrieved as
  indicated by the *Hive* metastore.
* *Data File/Path*: Data of the given format is retrieved directly from *Hadoop*
  or the local file system at the given location.

An example SSL configuration is available in the connector distribution, under
``src/test/resources/gpudb-secure.properties``.


### Template Tables

The *template table* feature is activated when ``table.use_templates`` is set to
``true``.  It provides a method for schema versioning when tables are created
with the loader.

To use this feature, a *template table* and collection must be created, where
the naming follows a specific pattern derived from the destination table name
and collection:

    collection = <collection>.template
    table = <table name>.<version string>

When searching for a schema, the loader will search for the pattern, sort
descending, and use the schema from the first result to create the destination
table.

For example, given a table ``test.avro_test``, the following set of schema
versions might exist:

    collection = test.template
    table = avro_test.20171220
    table = avro_test.20180130
    table = avro_test.20180230

When creating the table ``avro_test``, the loader will use the schema from
``avro_test.20180230`` because it shows up first in the reverse sort.


### Schema Merging

The *Spark DataFrame* and *Kinetica* table schemas may have different columns or
the columns may have different types. In this situation, the loader will apply
schema merging rules and build a mapping of source to destination columns.

The following rules apply when matching columns:

* Any source column with a case-sensitive name match to a destination column is
  mapped to that column
* Any unmapped column is ignored

If the column being mapped is numeric, a
[widening primitive conversion](https://docs.oracle.com/javase/specs/jls/se7/html/jls-5.html#jls-5.1.2)
is applied, if necessary. When converting types, the mapper will use the
associated Java type of each column for comparison. The following conversions
are permitted:

| Source Type | Destination Type
| :---        | :---
| Integer     | Long
| Float       | Double
| Boolean     | Integer
| Date        | Long

The following conditions will cause the mapping to fail:

* if a column is not mapped and is marked non-nullable in the destination table
* if a column is mapped and would result in a
  [narrowing primitive conversion](https://docs.oracle.com/javase/specs/jls/se7/html/jls-5.html#jls-5.1.3)

**NOTE:** If either condition is detected during setup, no workers are launched.


### Examples

The distribution contains example jobs for *Avro* and CSV data sets. They
contain the following files.

| File Name                                                       | Description
| :---                                                            | :---
| ``scripts/loader/run-spark-loader.sh``                          | Launcher script
| ``scripts/loader/gpudb.properties``                             | Common parameter file
| ``scripts/loader/csv-test.properties``                          | Top-level parameter file
| ``scripts/loader/csv-test``                                     | CSV data file containing 50 test records
| ``scripts/loader/avro-test.properties``                         | Top-level parameter file
| ``scripts/loader/avro-test``                                    | *Avro* data containing 1000 test records
| ``src/test/scala/com/kinetica/spark/SparkKineticaDriver.scala`` | Loader scripting example

To run an example, configure the ``gpudb.properties`` for the target
environment, and execute ``run-spark-loader.sh`` from within the
``scripts/loader`` directory, as shown below.

    <SPARK_CONNECTOR_HOME>/scripts/loader$ ./run-spark-loader.sh csv-test.properties
    Using master: local[8]
    + spark-submit --class com.kinetica.spark.SparkKineticaDriver
        --master 'local[8]' --deploy-mode client
        --packages com.databricks:spark-avro_2.10:4.0.0
        --driver-java-options -Dlog4j.configuration=file:/opt/spark-test/kinetica-spark163-6.2.1/scripts/loader/log4j.properties
        ../../target/kinetica-spark163-6.2.1-jar-with-dependencies.jar csv-test.properties
    [...]
    INFO  com.kin.spa.SparkKineticaDriver (SparkKineticaDriver.scala:112) - Reading properties from file: csv-test.properties
    INFO  org.apa.spa.SparkContext (Logging.scala:54) - Running Spark version 2.2.1
    [...]
    INFO  org.apa.spa.SparkContext (Logging.scala:54) - Successfully stopped SparkContext


## Spark Ingests Processor

The *Spark Ingest Processor* provides an easy API-level interface for
moving data between *Spark* and *Kinetica*.

Features include:

* Interfaces with *Kinetica* through *Spark DataFrames*

  * Optimizes datatype conversions between *Spark* and *Kinetica*
  * Automatic right-sizing of string and numeric fields

* Handles drifting/evolving schemas

  * Automatically adds new columns from *DataFrames* to existing *Kinetica*
    tables
  * Automatically widens *Kinetica* table columns to fit new data


### Architecture

The connector API will extract the data types from a *Spark* ``DataFrame`` and
construct a table in *Kinetica* with the corresponding schema.  The following
*Spark* datatypes are supported:

* ``NumericType``

    * ``ByteType``
    * ``ShortType``
    * ``IntegerType``
    * ``LongType``
    * ``FloatType``
    * ``DoubleType``
    * ``DecimcalType``

* ``StringType``
* ``BooleanType`` (converted to 1 or 0, integer)
* ``DateType``
* ``TimestampType``

The connector is case sensitive when it maps dataset column names to *Kinetica*
table column names.  This is an existing limitation with *Spark* ``row.getAs``.
Another option exposed via connector is to map data to columns based on
position.  If this is used, the number & order of columns must match between the
dataset and table.

The *Ingest Processor* will create the target table, if necessary, and then load
data into it.  Each *Spark* ``DataFrame`` partition instantiates a *Kinetica*
``BulkInserter``, which is a native API for rapid data ingest.


### Property Specification

*Ingest Processor* accepts properties programmatically, as format options.

In the examples here, map objects will be used for configuring the specifics of
processing and passed in as format options as a set.


### Data Ingest

1. Create a ``Map`` and initialize with appropriate connection config
2. Create a ``SparkSession``
3. Create a ``DataFrame`` with the data to load (the data types should match the
   schema of the table being loaded)
4. Write out the ``DataFrame`` using the *Kinetica* custom format and ``Map``
   options

**NOTE:**  To use the drifting/evolving schema option, set the
``table.append_new_columns`` parameter to ``true`` in the options ``Map``.


### Usage Considerations

* The connector does not perform any ETL transformations
* Data types must match between *Spark* and *Kinetica*, with the exception of
  *string* columns, which can be wider, if drifting/evolving schema support has
  been configured
* For row updates, columns not present during update will be set to ``null``
* Each ``Dataset`` partition should handle no fewer than 1-2 million records
* If LDAP/SSL is enabled, the connection string must point to the SSL URL and a
  valid certificate must be used


### Examples

These examples will demonstrate ingesting data into *Kinetica*

They make use of a 2008 airline data set, available here:

* <http://stat-computing.org/dataexpo/2009/2008.csv.bz2>

This example assumes the ``2008.csv`` and *Spark* connector JAR
(``kinetica-spark163-6.2.1-jar-with-dependencies.jar``) have been copied to the
``/opt/gpudb/connectors/spark`` directory on the *Spark* master node.


#### Ingest

The following example shows how to load data into *Kinetica* via ``DataFrame``.
It will first read airline data from CSV into a ``DataFrame``, and then load the
``DataFrame`` into *Kinetica*.


Launch *Spark Shell*:

```shell
$ spark-shell --packages com.databricks:spark-csv_2.10:1.5.0 --jars /opt/gpudb/connectors/spark/kinetica-spark163-6.2.1-jar-with-dependencies.jar
```

Configure loader for target database; be sure to provide an appropriate value
   for ``<KineticaHostName/IP>``, as well as ``<Username>`` & ``<Password>``, if
   the database is configured to require authentication:

```scala
val host = "<KineticaHostName/IP>"
val username = "<Username>"
val password = "<Password>"
val url = s"http://${host}:9191"
val options = Map(
   "database.url" -> url,
   "database.jdbc_url" -> s"jdbc:simba://${host}:9292;URL=${url}",
   "database.username" -> username,
   "database.password" -> password,
   "table.name" -> "airline",
   "table.create" -> "true",
   "table.truncate" -> "true",
   "table.is_replicated" -> "false",
   "table.update_on_existing_pk" -> "true",
   "table.map_columns_by_name" -> "false",
   "ingester.ip_regex" -> "",
   "ingester.batch_size" -> "10000",
   "ingester.num_threads" -> "4"
)
```

Read data from CSV file into ``DataFrame``:

```scala
val df = spark.read.
         format("csv").
         option("header", "true").
         option("inferSchema", "true").
         option("delimiter", ",").
         csv("/opt/gpudb/connectors/spark/2008.csv")
```

Write data from ``DataFrame`` into *Kinetica*:

```scala
df.write.format("com.kinetica.spark").options(options).save()
```

After the data load is complete, an ``airline`` table should exist in *Kinetica*
that matches the ``2008.csv`` data file.

The test JAR, ``kinetica-spark163-6.2.1-tests.jar``, created in the
*Build & Install* section, can be used to run the example above.  This command
assumes that the test JAR is also under ``/opt/gpudb/connectors/spark`` on the
*Spark* master node; be sure to provide appropriate values for
``<SparkMasterHostName/IP>``, ``<SparkMasterPort>``, &
``<KineticaHostName/IP>``, as well as ``<Username>`` & ``<Password>``, if
applicable:

```shell
$ spark-submit \
    --master "spark://<SparkMasterHostName/IP>:<SparkMasterPort>" \
    --class "com.kinetica.spark.KineticaIngestTest" \
    --jars /opt/gpudb/connectors/spark/kinetica-spark163-6.2.1-tests.jar \
       /opt/gpudb/connectors/spark/kinetica-spark163-6.2.1-jar-with-dependencies.jar \
       /opt/gpudb/connectors/spark/2008.csv \
       <KineticaHostName/IP> <Username> <Password>
```


#### PySpark

The following example shows how to load data into *Kinetica* via ``DataFrame``
using *PySpark*.  It will first read airline data from CSV into a ``DataFrame``,
and then load the ``DataFrame`` into *Kinetica*.


Launch *PySpark Shell*:

```shell
$ pyspark --jars /opt/gpudb/connectors/spark/kinetica-spark163-6.2.1-jar-with-dependencies.jar
```

Import *PySpark* resources:

```python
from pyspark.sql import SQLContext
```

Configure loader for target database; be sure to provide an appropriate value
   for ``<KineticaHostName/IP>``, as well as ``<Username>`` & ``<Password>``, if
   the database is configured to require authentication:

```python
host = "<KineticaHostName/IP>"
username = "<Username>"
password = "<Password>"
url = "http://%s:9191" % host
options = {
    "database.url" : url,
    "database.jdbc_url" : "jdbc:simba://%s:9292;URL=%s" % (host, url),
    "database.username" : username,
    "database.password" : password,
    "table.name" : "airline",
    "table.is_replicated" : "false",
    "table.map_columns_by_name" : "false",
    "table.create" : "true",
    "table.truncate" : "true"
}
```

Get *SQLContext*:

```python
sqlContext = SQLContext(sc)
```

Read data from CSV file into ``DataFrame``:

```python
df = sqlContext.read.load(
        '/opt/gpudb/connectors/spark/2008.csv',
        format='com.databricks.spark.csv',
        header='true',
        inferSchema='true',
        delimeter=','
     )
```

Write data from ``DataFrame`` into *Kinetica*:

```python
df.write.format("com.kinetica.spark").options(**options).save()
```

After the data load is complete, an ``airline`` table should exist in *Kinetica*
that matches the ``2008.csv`` data file.

The connector is packaged with a script that can run the above example, found
within the *Spark* connector home directory under
``scripts/python/kineticaingest.py``.  Be sure to provide appropriate values for
``<SparkMasterHostName/IP>``, ``<SparkMasterPort>``, &
``<KineticaHostName/IP>``, as well as ``<Username>`` & ``<Password>``, if
applicable.  ``<KineticaSparkConnectorHome>`` should be set to the *Spark*
connector home directory:

```shell
$ spark-submit \
    --master "spark://<SparkMasterHostName/IP>:<SparkMasterPort>" \
    --jars /opt/gpudb/connectors/spark/kinetica-spark163-6.2.1-jar-with-dependencies.jar \
       <KineticaSparkConnectorHome>/scripts/python/kineticaingest.py \
       /opt/gpudb/connectors/spark/2008.csv \
       <KineticaHostName/IP> <Username> <Password>
```

## Property Reference

This section describes properties used to configure the connector.  Many
properties are applicable to both connector modes; exceptions will be noted.


### Connection Properties

The following properties control the authentication & connection to *Kinetica*.

| Property Name                | Default   | Description
| :---                         | :---      | :---
| ``database.url``             | *<none>*  | URL of *Kinetica* instance (http or https)
| ``database.jdbc_url``        | *<none>*  | JDBC URL of the *Kinetica ODBC Server*  **Ingest Processor Only**
| ``database.stream_url``      | *<none>*  | ZMQ URL of the *Kinetica* table monitor  **Streaming Processor Only**
| ``database.username``        | *<none>*  | *Kinetica* login username
| ``database.password``        | *<none>*  | *Kinetica* login password
| ``database.retry_count``     | ``5``     | Connection retry count
| ``database.timeout_ms``      | ``10000`` | Connection timeout, in milliseconds
| ``ingester.multi_head``      | ``false`` | Enable multi-head ingest; this must be false for replicated tables
| ``ingester.ip_regex``        | *<none>*  | Regular expression to use in selecting *Kinetica* worker node IP addresses (e.g., ``172.*``) that are accessible by the connector, for multi-head ingest  **Ingest Processor Only**
| ``ingester.num_threads``     | ``4``     | Number of threads for bulk inserter
| ``ingester.batch_size``      | ``10000`` | Batch size for bulk inserter
| ``ingester.use_snappy``      | ``false`` | Use *snappy* compression during ingestion  **Ingest Processor Only**
| ``spark.rows_per_partition`` | *<none>*  | Number of records per partition *Spark* should segment data into before loading into *Kinetica*; if not specified, *Spark* will use the same number of partitions it used to retrieve the source data  **Data Loader Only**

The following apply for the *Data Loader* if SSL is used. A keystore or
truststore can be specified to override the default from the JVM.

| Property Name               | Default  | Description
| :---                        | :---     | :---
| ``ssl.truststore_jks``      | *<none>* | JKS trust store for CA certificate check
| ``ssl.truststore_password`` | *<none>* | Trust store password
| ``ssl.keystore_p12``        | *<none>* | PKCS#12 key store--only for 2-way SSL
| ``ssl.keystore_password``   | *<none>* | Key store password
| ``ssl.bypass_cert_check``   | *<none>* | Whether CA certificate check should be skipped

### Data Source/Target Properties

The following properties govern the *Kinetica* table being accessed, as well as
the access mechanism.

| Property Name                   | Default   | Description
| :---                            | :---      | :---
| ``table.name``                  | *<none>*  | *Kinetica* table to access
| ``table.create``                | ``false`` | Automatically create table if missing
| ``table.truncate``              | ``false`` | Truncate table if it exists
| ``table.is_replicated``         | ``false`` | Whether the target table is replicated or not  **Ingest Processor Only**
| ``table.update_on_existing_pk`` | ``false`` | If the target table, ``table.name``, has a primary key, update records in it with matching primary key values from records being ingested
| ``table.use_templates``         | ``false`` | Enable *template tables*; see [Template Tables](#template-tables) section for details  **Data Loader Only**
| ``table.truncate_to_size``      | ``false`` | Truncate charN strings to size.

For the *Data Loader*, the following properties specify the data source &
format.

| Property Name          | Default   | Description
| :---                   | :---      | :---
| ``source.sql_file``    | *<none>*  | File containing a SQL-compliant query to use to retrieve data from *Hive* or *Spark-SQL*
| ``source.data_path``   | *<none>*  | File or directory in Hadoop or the local filesystem containing source data
| ``source.data_format`` | *<none>*  | Indicates the format of the file(s) in ``source.data_path``
| ``source.csv_header``  | ``false`` | If format is CSV, whether the file has column headers or not; if ``true``, the column headers will be used as column names in creating the target table if it doesn't exist and mapping source fields to target columns if the table does exist.  If ``false``, columns will be mapped by position.

**NOTE:**  Supported formats include:

* ``avro``
* ``csv``
* ``json``
* ``orc``
* ``parquet``

For the *Ingest Processor*, the following properties govern
evolving/drifting schemas.

| Property Name                 | Default   | Description
| :---                          | :---      | :---
| ``table.map_columns_by_name`` | ``true``  | Whether the *Ingest Processor* should map ``DataFrame`` columns by name or position; if ``true``, columns in the ``DataFrame`` will be mapped in case-sensitive fashion to the target table; if ``false``, ``DataFrame`` columns will be mapped by position within the ``DataFrame`` to position within the target table
| ``table.append_new_columns``  | ``false`` | Whether the *Ingest Processor* should append columns from the source that don't exist in the target table to the target table
