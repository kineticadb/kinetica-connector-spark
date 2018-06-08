# Kinetica Spark Connector

This project contains the **6.2** version of the **Kinetica Spark Connector**
for bidirectional integration of *Kinetica* with *Spark*.

This guide exists on-line at:  [Kinetica Spark Connector Guide](http://www.kinetica.com/docs/6.2/connectors/spark_guide.html)
More information can be found at:  [Kinetica Documentation](http://www.kinetica.com/docs/6.2/index.html)

-----

# Kinetica Spark Connector Guide

The following guide provides step by step instructions to get started using
*Spark* with *Kinetica*.  The *Spark Connector* provides easy integration of
*Spark v2.x* with *Kinetica* via the *Spark Data Source API*.

There are two ways in which this connector can interface with *Kinetica*:

1. as a configurable [data loader](#spark-data-loader), via the command line
2. as an interactive
   [data ingest/egress processor](#spark-ingestegress-processor),
   programmatically, via the *Kinetica Spark API*

Source code for the connector can be found at:

* <https://github.com/kineticadb/kinetica-connector-spark>

## Contents

* [Build & Install](#build--install)
* [Usage](#usage)
* [Property Reference](#property-reference)
* [Spark Data Loader](#spark-data-loader)
* [Spark Ingest/Egress Processor](#spark-ingestegress-processor)



## Build & Install

The connector jar can be built with *Maven* as follows:

    $ git clone https://github.com/kineticadb/kinetica-connector-spark.git
    $ cd kinetica-connector-spark
    $ git fetch
    $ git checkout release/v6.2.0
    $ mvn clean package

**NOTE:**  Compilation requires Java 1.8. Ensure that ``JAVA_HOME`` is set
appropriately.

This should produce the connector jar under the ``target`` directory:

    target/kinetica-spark-6.2.1-jar-with-dependencies.jar

This jar should be made available to the *Spark* cluster upon submitting the
*Spark* job.

This will also produce a testing jar under the same directory:

    target/kinetica-spark-6.2.1-tests.jar

This JAR will be referenced later in this guide for use in testing the
*Spark* connector.


## Usage

To run the *Data Loader* on a *Spark* cluster:

    $ spark-submit \
       --class com.kinetica.spark.SparkKineticaDriver \
       --master "spark://<SparkMasterHostName/IP>:<SparkMasterPort>" \
       kinetica-spark-6.2.1-jar-with-dependencies.jar <PropertiesFile>


To launch the *Ingest/Egress Processor* through the *Spark* shell, run:

    $ spark-shell --jars kinetica-spark-6.2.1-jar-with-dependencies.jar


To run the *Ingest/Egress Processor* through the *PySpark* shell:

    $ pyspark --jars kinetica-spark-6.2.1-jar-with-dependencies.jar



## Property Reference

This section describes properties used to configure the connector.  Many
properties are applicable to both connector modes; exceptions will be noted.


### Connection Properties

The following properties control the authentication & connection to *Kinetica*.

| Property Name                | Default   | Description
| :---                         | :---      | :---
| ``database.url``             | *<none>*  | URL of *Kinetica* instance (http or https)
| ``database.jdbc_url``        | *<none>*  | JDBC URL of the *Kinetica ODBC Server*  **Ingest Processor Only**
| ``database.username``        | *<none>*  | *Kinetica* login username
| ``database.password``        | *<none>*  | *Kinetica* login password
| ``database.retry_count``     | ``5``     | Connection retry count  **Ingest Processor Only**
| ``database.timeout_ms``      | ``10000`` | Connection timeout, in milliseconds  **Data Loader Only**
| ``ingester.multi_head``      | ``false`` | Enable multi-head ingest; this must be false for replicated tables
| ``ingester.ip_regex``        | *<none>*  | Regular expression to use in selecting *Kinetica* worker node IP addresses (e.g., ``172.*``) that are accessible by the connector, for multi-head ingest  **Ingest Processor Only**
| ``ingester.num_threads``     | ``4``     | Number of threads for bulk inserter
| ``ingester.batch_size``      | ``10000`` | Batch size for bulk inserter
| ``ingester.use_snappy``      | ``false`` | Use *snappy* compression during ingestion  **Ingest Processor Only**
| ``spark.num_partitions``     | ``4``     | Number of *Spark* partitions to use for extracting data  **Egress Processor Only**
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

For the *Data Loader*, the following properties specify the data source &
format.

| Property Name          | Default  | Description
| :---                   | :---     | :---
| ``source.sql_file``    | *<none>* | File containing a SQL-compliant query to use to retrieve data from *Hive* or *Spark-SQL*
| ``source.data_path``   | *<none>* | File or directory in Hadoop or the local filesystem containing source data
| ``source.data_format`` | *<none>* | Indicates the format of the file(s) in ``source.data_path``

**NOTE:**  Supported formats include:

* ``avro``
* ``csv``
* ``json``
* ``orc``
* ``parquet``

For the *Ingest/Egress Processor*, the following properties govern
evolving/drifting schemas.

| Property Name                 | Default   | Description
| :---                          | :---      | :---
| ``table.map_columns_by_name`` | ``true``  | Whether the *Ingest Processor* should map ``DataFrame`` columns by name or position; if ``true``, columns in the ``DataFrame`` will be mapped in case-sensitive fashion to the target table; if ``false``, ``DataFrame`` columns will be mapped by position within the ``DataFrame`` to position within the target table
| ``table.append_new_columns``  | ``false`` | Whether the *Ingest Processor* should append columns from the source that don't exist in the target table to the target table


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
| ``scripts/loader/gpudb.properties``                             | Common parameter file
| ``scripts/loader/run-spark-loader.sh``                          | Launcher script
| ``scripts/loader/csv-test.properties``                          | Top-level parameter file
| ``scripts/loader/csv-test``                                     | CSV data file containing 50 test records
| ``scripts/loader/avro-test.properties``                         | Top-level parameter file
| ``scripts/loader/avro-test``                                    | *Avro* data containing 1000 test records
| ``scripts/loader/run-spark-loader.sh``                          | Launcher script
| ``src/test/scala/com/kinetica/spark/SparkKineticaDriver.scala`` | Loader scripting example

To run an example, configure the ``gpudb.properties`` for the target
environment, and execute ``run-spark-loader.sh``, as shown below.

    <SPARK_CONNECTOR_HOME>/scripts/loader$ ./run-spark-loader.sh csv-test.properties
    Using master: local[8]
    + spark-submit --class com.kinetica.spark.SparkKineticaDriver
        --master 'local[8]' --deploy-mode client
        --packages com.databricks:spark-avro_2.11:4.0.0
        --driver-java-options -Dlog4j.configuration=file:/opt/spark-test/kinetica-spark-6.2.1/scripts/loader/log4j.properties
        ../../target/kinetica-spark-6.2.1-jar-with-dependencies.jar csv-test.properties
    [...]
    INFO  com.kin.spa.SparkKineticaDriver (SparkKineticaDriver.scala:112) - Reading properties from file: csv-test.properties
    INFO  org.apa.spa.SparkContext (Logging.scala:54) - Running Spark version 2.2.0
    [...]
    INFO  org.apa.spa.SparkContext (Logging.scala:54) - Successfully stopped SparkContext


## Spark Ingest/Egress Processor

The *Spark Ingest/Egress Processor* provides an easy API-level interface for
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

Both the *Ingest Processor* & *Egress Processor* accept properties
programmatically, as format options.

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


### Data Egress

1. Create a ``Map`` and initialize with appropriate connection config
2. Create a ``SparkSession``
3. Load data from *Kinetica* into a ``DataFrame``, using the
   ``com.kinetica.spark`` read format with the session's ``sqlContext``

**NOTE:**  When using ``filter`` operations, the query will be split into the
number of partitions specified by ``spark.num_partitions`` in the configuration
``Map``. Each partition will pass the filtering operation to *Kinetica* to
perform and will only extract those *Kinetica*-filtered records.  Presently,
``filter`` is the only operation that takes advantage of this pass-down
optimization.


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

These examples will demonstrate ingesting data into *Kinetica*, extracting data
from *Kinetica*, and streaming data to & from *Kinetica*.

They make use of a 2008 airline data set, available here:

* <http://stat-computing.org/dataexpo/2009/2008.csv.bz2>

This example assumes the ``2008.csv`` and *Spark* connector JAR
(``kinetica-spark-6.2.1-jar-with-dependencies.jar``) are located in the
``/opt/gpudb/connectors/spark`` directory on the *Spark* master node.


#### Ingest

The following example shows how to load data into *Kinetica* via ``DataFrame``.
It will first read airline data from CSV into a ``DataFrame``, and then load the
``DataFrame`` into *Kinetica*.


1. Launch *Spark Shell*:

       $ spark-shell --jars /opt/gpudb/connectors/spark/kinetica-spark-6.2.1-jar-with-dependencies.jar

2. Import *Spark* resources:

       import org.apache.spark.SparkConf
       import org.apache.spark.sql.SparkSession

3. Configure loader for target database; be sure to provide an appropriate value
   for ``<KineticaHostName/IP>``:

       val host = "<KineticaHostName/IP>"
       val URL = s"http://${host}:9191"
       val options = Map(
           "database.url" -> URL,
           "database.jdbc_url" -> s"jdbc:simba://${host}:9292;URL=${URL};ParentSet=MASTER",
           "database.username" -> "",
           "database.password" -> "",
           "ingester.ip_regex" -> "",
           "ingester.batch_size" -> "10000",
           "ingester.num_threads" -> "4",
           "table.name" -> "airline",
           "table.is_replicated" -> "false",
           "table.update_on_existing_pk" -> "true",
           "table.map_columns_by_name" -> "false",
           "table.create" -> "true"
       )

4. Get *Spark* session:

       val conf = new SparkConf().setAppName("spark-custom-datasource")
       conf.set("spark.driver.userClassPathFirst" , "true")
       val spark = SparkSession.builder().config(conf).getOrCreate()

5. Read data from CSV file into ``DataFrame``:

       val df = spark.read.
                format("csv").
                option("header", "true").
                option("inferSchema", "true").
                option("delimiter", ",").
                csv("/opt/gpudb/connectors/spark/2008.csv")

6. Write data from ``DataFrame`` into *Kinetica*:

       df.write.format("com.kinetica.spark").options(options).save()


After the data load is complete, an ``airline`` table should exist in *Kinetica*
that matches the ``2008.csv`` data file.

The test JAR, ``kinetica-spark-6.2.1-tests.jar``, created in the
*Build & Install* section, can be used to run the example above.  This command
assumes that the test JAR is also under ``/opt/gpudb/connectors/spark`` on the
*Spark* master node; be sure to provide appropriate values for
``<SparkMasterHostName/IP>``, ``<SparkMasterPort>``, &
``<KineticaHostName/IP>``:

    $ spark-submit \
        --master "spark://<SparkMasterHostName/IP>:<SparkMasterPort>" \
        --class "com.kinetica.spark.KineticaIngestTest" \
        --jars /opt/gpudb/connectors/spark/kinetica-spark-6.2.1-tests.jar \
           /opt/gpudb/connectors/spark/kinetica-spark-6.2.1-jar-with-dependencies.jar \
           /opt/gpudb/connectors/spark/2008.csv \
           airline \
           <KineticaHostName/IP>


#### Egress

The following example shows how to extract data from *Kinetica* into a
``DataFrame``.  It will first read table data into a ``DataFrame`` and then
write that data out to a CSV file.  Lastly, it will run several operations on
the ``DataFrame`` and output the results to the console.


1. Launch *Spark Shell*:

       $ spark-shell --jars /opt/gpudb/connectors/spark/kinetica-spark-6.2.1-jar-with-dependencies.jar

2. Import *Spark* resources:

       import org.apache.spark.SparkConf
       import org.apache.spark.sql.SparkSession
       import org.apache.spark.sql.functions

3. Configure processor for source database; be sure to provide an appropriate
   value for ``<KineticaHostName/IP>``:

       val host = "<KineticaHostName/IP>"
       val URL = s"http://${host}:9191"
       val options = Map(
           "database.jdbc_url" -> s"jdbc:simba://${host}:9292;URL=${URL};ParentSet=MASTER",
           "database.username" -> "",
           "database.password" -> "",
           "spark.num_partitions" -> "8",
           "table.name" -> "airline"
       )

4. Get *Spark* session:

       val conf = new SparkConf().setAppName("spark-custom-datasource")
       conf.set("spark.driver.userClassPathFirst" , "true")
       val spark = SparkSession.builder().config(conf).getOrCreate()
       val sqlContext = spark.sqlContext

5. Read filtered data from *Kinetica* into ``DataFrame`` (July 2008 data only):

       val df = sqlContext.read.format("com.kinetica.spark").options(options).load().filter("Month = 7")
         
6. Write data from ``DataFrame`` to CSV:

       df.write.format("csv").save("/opt/gpudb/connectors/spark/2008.july")

7. Aggregate data and output statistics:

       println(
          df.
             groupBy("DayOfWeek").
             agg(
                functions.count("*").as("TotalFlights"),
                functions.sum("Diverted").as("TotalDiverted"),
                functions.sum("Cancelled").as("TotalCancelled")
             ).
             orderBy("DayOfWeek").
             show()
       )

8. Verify output:

       +---------+------------+-------------+--------------+
       |DayOfWeek|TotalFlights|TotalDiverted|TotalCancelled|
       +---------+------------+-------------+--------------+
       |        1|       84095|          120|          1289|
       |        2|      103429|          417|          1234|
       |        3|      103315|          367|          2313|
       |        4|      105035|          298|          1936|
       |        5|       79349|          120|           903|
       |        6|       72219|          174|           570|
       |        7|       80489|          414|          2353|
       +---------+------------+-------------+--------------+


After the data write is complete, a ``2008.july`` directory should have been
created, containing all data from the ``airline`` table for the month of July.

The test JAR, ``kinetica-spark-6.2.1-tests.jar``, created in the
*Build & Install* section, can be used to run the example above.  This command
assumes that the test JAR is also under ``/opt/gpudb/connectors/spark`` on the
*Spark* master node; be sure to provide appropriate values for
``<SparkMasterHostName/IP>``, ``<SparkMasterPort>``, &
``<KineticaHostName/IP>``:

    $ spark-submit \
        --master "spark://<SparkMasterHostName/IP>:<SparkMasterPort>" \
        --class "com.kinetica.spark.KineticaEgressTest" \
        --jars /opt/gpudb/connectors/spark/kinetica-spark-6.2.1-tests.jar \
           /opt/gpudb/connectors/spark/kinetica-spark-6.2.1-jar-with-dependencies.jar \
           /opt/gpudb/connectors/spark/2008.july \
           airline \
           <KineticaHostName/IP>


#### PySpark

The following example shows how to load data into *Kinetica* via ``DataFrame``
using *PySpark*.  It will first read airline data from CSV into a ``DataFrame``,
and then load the ``DataFrame`` into *Kinetica*.


1. Launch *PySpark Shell*:

       $ pyspark --jars /opt/gpudb/connectors/spark/kinetica-spark-6.2.1-jar-with-dependencies.jar

2. Import *PySpark* resources:

       from pyspark.sql import SQLContext

3. Configure loader for target database; be sure to provide an appropriate value
   for ``<KineticaHostName/IP>``:

       host = "<KineticaHostName/IP>"
       URL = "http://%s:9191" % host
       options = {
           "database.url" : URL,
           "database.jdbc_url" : "jdbc:simba://%s:9292;URL=%s;ParentSet=MASTER" % (host, URL),
           "table.name" : "airline",
           "table.is_replicated" : "false",
           "table.map_columns_by_name" : "false",
           "table.create" : "true"
       }

4. Get *SQLContext*:

       sqlContext = SQLContext(sc)

5. Read data from CSV file into ``DataFrame``:

       df = sqlContext.read.load(
               '/opt/gpudb/connectors/spark/2008.csv',
               format='com.databricks.spark.csv',
               header='true',
               inferSchema='true',
               delimeter=','
            )

6. Write data from ``DataFrame`` into *Kinetica*:

       df.write.format("com.kinetica.spark").options(**options).save()


After the data load is complete, an ``airline`` table should exist in *Kinetica*
that matches the ``2008.csv`` data file.

The connector is packaged with a script that can run the above example, found
under ``scripts/python/kineticaingest.py``.  This command assumes that that
*Python* script is available at that path relative to where the command is
executed; be sure to provide appropriate values for
``<SparkMasterHostName/IP>``, ``<SparkMasterPort>``, &
``<KineticaHostName/IP>``:

    $ spark-submit \
        --master "spark://<SparkMasterHostName/IP>:<SparkMasterPort>" \
        --jars /opt/gpudb/connectors/spark/kinetica-spark-6.2.1-jar-with-dependencies.jar \
           scripts/python/kineticaingest.py \
           /opt/gpudb/connectors/spark/2008.csv \
           airline \
           <KineticaHostName/IP>
