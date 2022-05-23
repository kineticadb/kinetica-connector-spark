# Kinetica Spark Connector

This project contains the **7.1** version of the **Legacy Kinetica Spark Connector**
for bidirectional integration of *Kinetica* with *Spark*.

This guide exists on-line at:  [Kinetica Spark Connector Guide](https://docs.kinetica.com/7.1/connectors/spark_guide.html)

More information can be found at:  [Kinetica Documentation](https://docs.kinetica.com/7.1/index.html)

-----

# Kinetica Spark Guide

The following guide provides step by step instructions to get started using
*Spark* with *Kinetica*.

There are two primary methods by which *Kinetica* & *Spark* can be integrated:

* [SQL via JDBC](#sql) *(Preferred)*
* [Legacy Spark Connector](#legacy-spark-connector) *(Deprecated)*

**NOTE:**  The *Spark Connector* has been deprecated in favor of SQL via the
           *Kinetica* JDBC driver.  For alternative means of rapid ingest, see:

* SQL [CREATE EXTERNAL TABLE](https://docs.kinetica.com/7.1/concepts/sql/#create-external-table)
* SQL [LOAD INTO](https://docs.kinetica.com/7.1/concepts/sql/#load-into)
* [Multi-Head Ingest](https://docs.kinetica.com/7.1/tuning/multihead/multihead_ingest/)


## SQL

SQL queries can be issued against *Kinetica* through the *Spark* JDBC interface.
This allows access to native *Kinetica* functions, including geospatial
operations.  These queries will not be partitioned, however, like queries made
through the *Egress Processor* of the *Legacy Spark Connector*.

See [Connecting via JDBC](https://docs.kinetica.com/7.1/connectors/sql_guide/#java-client-via-jdbc)
for obtaining the JDBC driver.

The following example shows how to execute queries against *Kinetica*.  It will
use JDBC as the read format, require the *Kinetica* JDBC driver to be accessible
and load it, and allow the specified query to run.  The result of the query will
be loaded into a ``DataFrame`` and the schema and result set will be output to
the console.

This example makes use of the NYC taxi trip table, which can be loaded using
*GAdmin* from the *Demo Data* page, under *Cluster* > *Demo*.

Launch *Spark Shell*:

```shell
$ spark-shell --jars kinetica-jdbc-7.1.*-jar-with-dependencies.jar
```

Configure JDBC for source database and specify query for map key ``dbtable``;
be sure to provide an appropriate value for ``<KineticaHostName/IP>``, as
well as ``<Username>`` & ``<Password>``, if the database is configured to
require authentication.

   
**NOTE:**  If connecting over SSL, see
           [JDBC Secure Connections](https://docs.kinetica.com/7.1/connectors/sql_guide/#odbc-connecting-secure)
           for the modified URL to use.

```scala
val host = "<KineticaHostName/IP>"
var url = s"jdbc:kinetica://${host}:9191"
val username = "<Username>"
val password = "<Password>"
val options = Map(
   "url" -> url,
   "driver" -> "com.kinetica.jdbc.Driver",
   "UID" -> username,
   "PWD" -> password,
   "dbtable" -> s"""(
      SELECT
         vendor_id,
         DECIMAL(MIN(geo_miles)) AS min_geo_miles,
         DECIMAL(AVG(geo_miles)) AS avg_geo_miles,
         DECIMAL(MAX(geo_miles)) AS max_geo_miles
      FROM
      (
         SELECT
            vendor_id,
            DECIMAL(GEODIST(pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude) * 0.000621371) AS geo_miles
         FROM demo.nyctaxi
      )
      WHERE geo_miles BETWEEN .01 AND 100
      GROUP BY vendor_id
   )"""
)
```

Get *Spark* SQL context:

```scala
val sqlContext = spark.sqlContext
```

Read queried data from *Kinetica* into ``DataFrame``:

```scala
val df = sqlContext.read.format("jdbc").options(options).load()
```

Output ``DataFrame`` schema for query:

```scala
df.printSchema
```

Verify output:

    root
     |-- vendor_id: string (nullable = true)
     |-- min_geo_miles: decimal(20,4) (nullable = true)
     |-- avg_geo_miles: decimal(20,4) (nullable = true)
     |-- max_geo_miles: decimal(20,4) (nullable = true)

Output query result set:

```scala
df.orderBy("vendor_id").show
```

Verify output (may contain additional records from streaming test):

    +---------+-------------+-------------+-------------+
    |vendor_id|min_geo_miles|avg_geo_miles|max_geo_miles|
    +---------+-------------+-------------+-------------+
    |      CMT|       0.0100|       2.0952|      80.8669|
    |      DDS|       0.0148|       2.7350|      64.2944|
    |      NYC|       0.0101|       2.1548|      36.9236|
    |      VTS|       0.0100|       2.0584|      94.5213|
    |     YCAB|       0.0100|       2.1049|      36.0565|
    +---------+-------------+-------------+-------------+


### Mapping Spark to Kinetica

Some *Spark* data types and functions may need custom mappings to *Kinetica*.

The following dialect snippet is a custom mapping, which maps:

* *Spark's* CLOB/VARCHAR types to the *Kinetica* ``VARCHAR`` type
* *Spark's* BLOB type to the *Kinetica* ``BLOB`` type
* *Spark's* boolean type to the *Kinetica* ``TINYINT`` type
* The truncate command (which does ``DROP``/``CREATE``, by default) to
  *Kinetica's* ``TRUNCATE TABLE`` command

```scala
import org.apache.spark.sql.jdbc.{JdbcDialects, JdbcDialect, JdbcType}
import java.sql.Types
import java.util.Locale

import org.apache.spark.sql.types._

val KineticaDialect = new JdbcDialect {
    override def canHandle(url: String): Boolean =
        url.toLowerCase(Locale.ROOT).startsWith("jdbc:kinetica")
    override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
        case StringType => Some(JdbcType("TEXT", java.sql.Types.VARCHAR))
        case BinaryType => Some(JdbcType("VARBINARY", java.sql.Types.VARBINARY))
        case BooleanType => Option(JdbcType("TINYINT", java.sql.Types.TINYINT))
        case _ => None
    }
    override def isCascadingTruncateTable(): Option[Boolean] = Some(false)
    override def getTruncateQuery(
        table: String,
        cascade: Option[Boolean] = isCascadingTruncateTable
    ): String = { s"TRUNCATE TABLE $table" }
}
JdbcDialects.registerDialect(KineticaDialect)
```

After running the application code, the dialect can be unregistered as follows:

```scala
JdbcDialects.unregisterDialect(KineticaDialect)
```



## Legacy Spark Connector

**Important:**  This connector has been deprecated.  See note at
                [top](#kinetica-spark-guide).

The legacy *Spark Connector* provides easy integration of
*Spark v2.3.x* with *Kinetica* via the *Spark Data Source API*.

There are two packages in this project:

* ``com.kinetica.spark.datasourcev1`` -- uses the *Spark DataSource v1 API*
* ``com.kinetica.spark.datasourcev2`` -- uses the *Spark DataSource v2 API*

The ``com.kinetica.spark`` package uses the v1 API by default.  The
*Spark DataSource v2 API* is still evolving, so we encourage users to use the v1
API (which can be used by default, or by explicitly choosing the first
aforementioned package).


There are three ways in which this connector can interface with *Kinetica*:

1. as a configurable [data loader](#spark-data-loader), via the command line,
   which can load data into *Kinetica* via *Spark*
2. as an interactive
   [data ingest/egress processor](#spark-ingestegress-processor),
   programmatically, via the *Kinetica Spark API*, which can ingest data into
   *Kinetica* from *Spark* or egress data from *Kinetica* into *Spark*
3. as an interactive
   [streaming data processor](#spark-streaming-processor),
   programmatically, via the *Kinetica Spark API*, which can stream data from
   *Kinetica* into *Spark*

Source code for the connector can be found at:

* <https://github.com/kineticadb/kinetica-connector-spark>

### Contents

* [Build & Install](#build--install)
* [Usage](#usage)
* [Spark Data Loader](#spark-data-loader)
* [Spark Ingest/Egress Processor](#spark-ingestegress-processor)
* [Spark Streaming Processor](#spark-streaming-processor)
* [SQL](#sql)
* [Federated Queries](#federated-queries)
* [Property Reference](#property-reference)



### Build & Install

The connector JAR can be built with *Maven* as follows:

    $ git clone https://github.com/kineticadb/kinetica-connector-spark.git -b release/v7.1 --single-branch
    $ cd kinetica-connector-spark
    $ mvn clean package -DskipTests

**NOTE:**  Compilation requires Java 1.8. Ensure that ``JAVA_HOME`` is set
appropriately.


This sequence produces the connector JAR, which will be made available to the
*Spark* cluster upon submitting the *Spark* job.  It can be found under the
``target`` directory:

    target/kinetica-spark-7.1.<X>.<Y>-jar-with-dependencies.jar

It will also produce at the same location the connector JAR with shaded package
`com.typesafe.scalalogging` dependency that can be used instead in enterprise-level
*Spark* environment with other third-party deployments that can have a potential
conflict with scalalogging library version:

    target/kinetica-spark-7.1.<X>.<Y>-jar-with-shaded-scalalogging.jar

It will also produce a testing JAR under the same directory, which will be
referenced later in this guide for use in testing the *Spark* connector:

    target/kinetica-spark-7.1.<X>.<Y>-tests.jar

In order to run the pre-packaged tests, run:

    $ mvn test -Dkurl=http://<KINETICA_IP>:<KINETICA_PORT> \
        -Dkusername=<kinetica_username> -Dkpassword=<kinetica_password>

**NOTE:** The tests fail with Java 1.9+ due to a known bug in Spark
(https://issues.apache.org/jira/browse/SPARK-24201).  Please use Java 1.8
for running the tests.


### Usage

To run the *Data Loader* on a *Spark* cluster, run the following command; be
sure to provide appropriate values for ``<SparkMasterHostName/IP>``,
``<SparkMasterPort>``, & ``<PropertiesFile>``:

    $ spark-submit \
       --class com.kinetica.spark.SparkKineticaDriver \
       --master "spark://<SparkMasterHostName/IP>:<SparkMasterPort>" \
       kinetica-spark-7.1.<X>.<Y>-jar-with-dependencies.jar <PropertiesFile>


To launch the *Ingest/Egress Processor* or *Streaming Processor* through the
*Spark* shell, run:

    $ spark-shell --jars kinetica-spark-7.1.<X>.<Y>-jar-with-dependencies.jar


To run the *Ingest/Egress Processor* through the *PySpark* shell:

    $ pyspark --jars kinetica-spark-7.1.<X>.<Y>-jar-with-dependencies.jar


### Spark Data Loader

**Important:**  This loader has been deprecated.  See note at
                [top](#kinetica-spark-guide).

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


#### Property Specification

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


#### Template Tables

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


#### Schema Merging

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


#### Examples

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
        --packages com.databricks:spark-avro_2.11:4.0.0
        --driver-java-options -Dlog4j.configuration=file:/opt/spark-test/kinetica-spark-7.1.<X>.<Y>/scripts/loader/log4j.properties
        ../../target/kinetica-spark-7.1.<X>.<Y>-jar-with-dependencies.jar csv-test.properties
    [...]
    INFO  com.kin.spa.SparkKineticaDriver (SparkKineticaDriver.scala:112) - Reading properties from file: csv-test.properties
    INFO  org.apa.spa.SparkContext (Logging.scala:54) - Running Spark version 2.2.1
    [...]
    INFO  org.apa.spa.SparkContext (Logging.scala:54) - Successfully stopped SparkContext


### Spark Ingest/Egress Processor

**Important:**  This processor has been deprecated.  See note at
                [top](#kinetica-spark-guide).

The *Spark Ingest/Egress Processor* provides an easy API-level interface for
moving data between *Spark* and *Kinetica*.

It is designed to interface with *Kinetica* through *Spark DataFrames*, and
optimize data type conversions between the two.


#### Architecture

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

For the *Ingest Processor*, each *Spark* ``DataFrame`` partition instantiates a
*Kinetica* ``BulkInserter``, which is a native API for rapid data ingest.


#### Property Specification

Both the *Ingest Processor* & *Egress Processor* accept properties
programmatically, as format options.

In the examples here, map objects will be used for configuring the specifics of
processing and passed in as format options as a set.


#### Data Ingest

##### Process Flow

1. Create a ``Map`` and initialize with appropriate connection config
2. Create a ``SparkSession``
3. Create a ``DataFrame`` with the data to load (the data types should match the
   schema of the table being loaded)
4. Write out the ``DataFrame`` using the *Kinetica* custom format and ``Map``
   options

##### Creating Schemas

The *Ingest Processor* can create the target table, if necessary, and then load
data into it.  It will perform automatic right-sizing of string and numeric
fields when creating column types & sizes.

To use the automatic schema creation option, set the ``table.create`` parameter
to ``true`` in the options ``Map``.

##### Complex Data Types

The *Ingest Processor* is able to perform conversions on several complex data
types to fit them into a single target table.  The following complex types are
supported:

* [Struct](#struct)
* [Array](#array)
* [Map](#map)

To use the complex data type conversion option, set the
``ingester.flatten_source_schema`` parameter to ``true`` in the options ``Map``.

###### Struct

Each leaf node of a *struct* will result in a single column in the target table.
The column's name will be the concatenation of the attribute at each level of
the *struct's* hierarchy leading to the leaf node, separated by underscores.

For example, given this schema containing a *struct*:

    root
     |-- customer_name: string (nullable = true)
     |-- customer_address: struct (nullable = true)
     |    |-- street: struct (nullable = true)
     |    |    |-- number: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |    |    |-- unit: string (nullable = true)
     |    |-- city: string (nullable = true)
     |    |-- state: string (nullable = true)
     |    |-- zip: string (nullable = true)

This schema may be derived (depending on the sizes of the data values):

```sql
CREATE TABLE customer
(
    customer_name VARCHAR(50),
    customer_address_street_number VARCHAR(5),
    customer_address_street_name VARCHAR(30),
    customer_address_street_unit VARCHAR(5),
    customer_address_city VARCHAR(30),
    customer_address_state VARCHAR(2),
    customer_address_zip VARCHAR(10)
)
```

###### Array

Each *array* will result in a single column in the target table, named the same
as the *array* field.  Each element in the *array* will result in a separate
record being inserted into the database.  All of the other column values will be
duplicated for each array element record.

For example, given this schema containing an *array*:

    root
     |-- customer_name: string (nullable = true)
     |-- customer_order_number: array (nullable = true)
     |    |-- element: integer (containsNull = true)

...and this data set:

```javascript
{
    {
        "customer_name": "John",
        "customer_order_number": [1,2,4]
    },
    {
        "customer_name": "Mary",
        "customer_order_number": [3,5]
    }
}
```

This table will be created:

    +-------------+---------------------+
    |customer_name|customer_order_number|
    +-------------+---------------------+
    |John         |                    1|
    |John         |                    2|
    |John         |                    4|
    |Mary         |                    3|
    |Mary         |                    5|
    +-------------+---------------------+

###### Map

Each unique *map key* will result in a single column in the target table.  Each
column's name is derived from the *map* name and *map key*, separated by an
underscore.  For a given record, *map values* will be populated in their
respective columns, while columns lacking corresponding *map values* will be set
to null.

For example, given this schema containing a *map*:

    root
     |-- customer_name: string (nullable = true)
     |-- customer_phone: map (nullable = true)
     |    |-- key: string
     |    |-- value: string (valueContainsNull = false)

...and this data set:

```javascript
{
    {
        "customer_name": "John",
        "customer_phone":
        {
            "home": "111-111-1111",
            "cell": "222-222-2222"
        }
    },
    {
        "customer_name": "Mary",
        "customer_phone":
        {
            "cell": "333-333-3333",
            "work": "444-444-4444"
        }
    }
}
```

This table will be created:

    +-------------+-------------------+-------------------+-------------------+
    |customer_name|customer_phone_home|customer_phone_work|customer_phone_cell|
    +-------------+-------------------+-------------------+-------------------+
    |John         |111-111-1111       |                   |222-222-2222       |
    |Mary         |                   |444-444-4444       |333-333-3333       |
    +-------------+-------------------+-------------------+-------------------+

##### Drifting/Evolving Schemas

The *Ingest Processor* can handle drifting/evolving schemas:

* Automatically adding new columns from *DataFrames* to existing *Kinetica*
  tables
* Automatically widening *Kinetica* table columns to fit new data

To use the drifting/evolving schema option, set the ``table.append_new_columns``
parameter to ``true`` in the options ``Map``.


#### Data Egress

##### Process Flow

1. Create a ``Map`` and initialize with appropriate connection config
2. Create a ``SparkSession``
3. Load data from *Kinetica* into a ``DataFrame``, using the
   ``com.kinetica.spark`` read format with the session's ``sqlContext``

##### Filter Pass-Down

When using ``filter`` operations, the query will be split into the number of
partitions specified by ``spark.num_partitions`` in the configuration ``Map``.
Each partition will pass the filtering operation to *Kinetica* to perform and
will only extract those *Kinetica*-filtered records.  Presently, ``filter`` is
the only operation that takes advantage of this pass-down optimization.


#### Usage Considerations

* The connector does not perform any ETL transformations
* Data types must match between *Spark* and *Kinetica*, with the exception of
  *string* columns, which can be wider, if drifting/evolving schema support has
  been configured
* For row updates, columns not present during update will be set to ``null``
* Each ``Dataset`` partition should handle no fewer than 1-2 million records
* If LDAP/SSL is enabled, the connection string must point to the SSL URL and a
  valid certificate must be used


#### Examples

These examples will demonstrate ingesting data into *Kinetica*, extracting data
from *Kinetica*, and using *PySpark* with *Kinetica*.

They make use of a 2008 airline data set, available here:

* <http://stat-computing.org/dataexpo/2009/2008.csv.bz2>

This example assumes the ``2008.csv`` and *Spark* connector JAR
(``kinetica-spark-7.1.<X>.<Y>-jar-with-dependencies.jar``) have been copied to
the ``/opt/gpudb/connectors/spark`` directory on the *Spark* master node.


##### Analyze Data

Before loading data into the database, an analysis of the data to ingest can be
done.  This will scan through the source data to determine what the target table
column types & sizes should be, and output the resulting ``CREATE TABLE``
statement without creating the table or loading any data.

To execute a data analysis, the ``ingester.analyze_data_only`` property must be
set to ``true``.  All other properties are ignored, and no connectivity to a
Kinetica database instance is required.

The following example shows how to perform a data analysis via ``DataFrame``.
It will read airline data from CSV into a ``DataFrame`` and write the schema, as
a ``CREATE TABLE`` statement, to the *Spark* log file (or console, depending on
*log4j* configuration).


Launch *Spark Shell*:

```shell
$ spark-shell --jars /opt/gpudb/connectors/spark/kinetica-spark-7.1.*-jar-with-dependencies.jar
```

Configure loader for target database; be sure to provide an appropriate value
for ``<KineticaHostName/IP>``, as well as ``<Username>`` & ``<Password>``, if
the database is configured to require authentication:

```scala
val options = Map(
    "ingester.analyze_data_only" -> "true"
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

Derive schema from ``DataFrame``, and log schema:

```scala
df.write.format("com.kinetica.spark").options(options).save()
```

**NOTE:**  In order to use the *Spark DataSource v2 API*, use the
           `com.kinetica.spark.datasourcev2` package instead.

After this is complete, the log should contain the ``CREATE TABLE`` statement
for the table appropriate for the airline dataset contained in :file:`2008.csv`.


##### Ingest

The following example shows how to load data into *Kinetica* via ``DataFrame``.
It will first read airline data from CSV into a ``DataFrame``, and then load the
``DataFrame`` into *Kinetica*.


Launch *Spark Shell*:

```shell
$ spark-shell --jars /opt/gpudb/connectors/spark/kinetica-spark-7.1.*-jar-with-dependencies.jar
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
   "database.jdbc_url" -> s"jdbc:kinetica://${host}:9191",
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

**NOTE:**  In order to use the *Spark DataSource v2 API*, use the
           `com.kinetica.spark.datasourcev2` package instead.

After the data load is complete, an ``airline`` table should exist in *Kinetica*
that matches the ``2008.csv`` data file.

The test JAR, ``kinetica-spark-7.1.<X>.<Y>-tests.jar``, created in the
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
    --jars /opt/gpudb/connectors/spark/kinetica-spark-7.1.*-tests.jar \
       /opt/gpudb/connectors/spark/kinetica-spark-7.1.*-jar-with-dependencies.jar \
       /opt/gpudb/connectors/spark/2008.csv \
       <KineticaHostName/IP> <Username> <Password>
```


##### Egress

The following example shows how to extract data from *Kinetica* into a
``DataFrame``.  It will first read table data into a ``DataFrame`` and then
write that data out to a CSV file.  Lastly, it will run several operations on
the ``DataFrame`` and output the results to the console.


Launch *Spark Shell*:

```shell
$ spark-shell --jars /opt/gpudb/connectors/spark/kinetica-spark-7.1.*-jar-with-dependencies.jar
```

Import *Spark* resources:

```scala
import org.apache.spark.sql.functions
```

Configure processor for source database; be sure to provide an appropriate
value for ``<KineticaHostName/IP>``, as well as ``<Username>`` &
``<Password>``, if the database is configured to require authentication:

```scala
val host = "<KineticaHostName/IP>"
val username = "<Username>"
val password = "<Password>"
val url = s"http://${host}:9191"
val options = Map(
   "database.url" -> url,
   "database.jdbc_url" -> s"jdbc:kinetica://${host}:9191",
   "database.username" -> username,
   "database.password" -> password,
   "spark.num_partitions" -> "8",
   "table.name" -> "airline"
)
```

Get *Spark* SQL context:

```scala
val sqlContext = spark.sqlContext
```

Read filtered data from *Kinetica* into ``DataFrame`` (July 2008 data only):

```scala
val df = sqlContext.read.format("com.kinetica.spark").options(options).load().filter("Month = 7")
```

Write data from ``DataFrame`` to CSV:

```scala
df.write.format("csv").mode("overwrite").save("2008.july")
```

Aggregate data and output statistics:

```scala
df.
   groupBy("DayOfWeek").
   agg(
      count("*").as("TotalFlights"),
      sum("Diverted").as("TotalDiverted"),
      sum("Cancelled").as("TotalCancelled")
   ).
   orderBy("DayOfWeek").
   select(
      when(df("DayOfWeek") === 1, "Monday").
      when(df("DayOfWeek") === 2, "Tuesday").
      when(df("DayOfWeek") === 3, "Wednesday").
      when(df("DayOfWeek") === 4, "Thursday").
      when(df("DayOfWeek") === 5, "Friday").
      when(df("DayOfWeek") === 6, "Saturday").
      when(df("DayOfWeek") === 7, "Sunday").alias("DayOfWeek"),
      column("TotalFlights"),
      column("TotalDiverted"),
      column("TotalCancelled")
   ).
   show()
```

Verify output:

    +---------+------------+-------------+--------------+
    |DayOfWeek|TotalFlights|TotalDiverted|TotalCancelled|
    +---------+------------+-------------+--------------+
    |   Monday|       84095|          120|          1289|
    |  Tuesday|      103429|          417|          1234|
    |Wednesday|      103315|          367|          2313|
    | Thursday|      105035|          298|          1936|
    |   Friday|       79349|          120|           903|
    | Saturday|       72219|          174|           570|
    |   Sunday|       80489|          414|          2353|
    +---------+------------+-------------+--------------+


After the data write is complete, a ``2008.july`` directory should have been
created, containing all data from the ``airline`` table for the month of July.

The test JAR, ``kinetica-spark-7.1.<X>.<Y>-tests.jar``, created in the
*Build & Install* section, can be used to run the example above.  This command
assumes that the test JAR is also under ``/opt/gpudb/connectors/spark`` on the
*Spark* master node; be sure to provide appropriate values for
``<SparkMasterHostName/IP>``, ``<SparkMasterPort>``, &
``<KineticaHostName/IP>``, as well as ``<Username>`` & ``<Password>``, if
applicable:

```shell
$ spark-submit \
    --master "spark://<SparkMasterHostName/IP>:<SparkMasterPort>" \
    --class "com.kinetica.spark.KineticaEgressTest" \
    --jars /opt/gpudb/connectors/spark/kinetica-spark-7.1.*-tests.jar \
       /opt/gpudb/connectors/spark/kinetica-spark-7.1.*-jar-with-dependencies.jar \
       <KineticaHostName/IP> <Username> <Password>
```

##### PySpark

The following example shows how to load data into *Kinetica* via ``DataFrame``
using *PySpark*.  It will first read airline data from CSV into a ``DataFrame``,
and then load the ``DataFrame`` into *Kinetica*.


Launch *PySpark Shell*:

```shell
$ pyspark --jars /opt/gpudb/connectors/spark/kinetica-spark-7.1.*-jar-with-dependencies.jar
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
    "database.jdbc_url" : "jdbc:kinetica://%s:9191" % (host),
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
    --jars /opt/gpudb/connectors/spark/kinetica-spark-7.1.*-jar-with-dependencies.jar \
       <KineticaSparkConnectorHome>/scripts/python/kineticaingest.py \
       /opt/gpudb/connectors/spark/2008.csv \
       <KineticaHostName/IP> <Username> <Password>
```


### Spark Streaming Processor

**Important:**  This processor has been deprecated.  See note at
                [top](#kinetica-spark-guide).

The *Spark Streaming Processor* provides an easy API-level interface for
streaming data from *Kinetica* to *Spark*.


#### Architecture

The connector API creates a *table monitor* in *Kinetica*, which will watch for
record inserts into a given table and publish them on a *ZMQ* topic.  A *Spark*
*DStream* will be established, which subscribes to that topic and makes those
added records available to the API user within *Spark*.

*ZMQ* runs on the *Kinetica* head node on the default port of ``9002``.


#### Property Specification

The *Streaming Processor* accepts properties programmatically, via
``LoaderParams``.

In the examples here, map objects will be used for configuring the specifics of
processing and passed in to ``LoaderParams``.


#### Establishing a Data Stream

1. Create a ``LoaderParams`` and initialize with appropriate connection config.
2. Create a ``StreamingContext``.
3. Create a *table monitor* and new record *ZMQ* topic with ``GPUdbReceiver``.
4. Create a ``DStream``, subscribing to the new record topic.


#### Usage Considerations

* The *table monitor* only watches for record inserts; thus, the ``DStream``
  will only contain table inserts, not updates or deletions.
* All new records will enter the queue topic via the *head node*; multi-head
  streaming is not supported at this time.


#### Examples

This example will demonstrate streaming data to & from *Kinetica*.

It makes use of a 2008 airline data set, available here:

* <http://stat-computing.org/dataexpo/2009/2008.csv.bz2>

A table will be created from that data, and a streaming monitor will be applied.
As new records are added to that table, batches of streamed records will be
represented in the *Spark* console.

This example assumes the ``2008.csv`` and *Spark* connector JAR
(``kinetica-spark-7.1.<X>.<Y>-jar-with-dependencies.jar``) have been copied to the
``/opt/gpudb/connectors/spark`` directory on the *Spark* master node.


Launch *Spark Shell*:

```shell
$ spark-shell --jars /opt/gpudb/connectors/spark/kinetica-spark-7.1.*-jar-with-dependencies.jar
```

Import *Spark* resources:

```scala
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import com.kinetica.spark.LoaderParams
import com.kinetica.spark.streaming._
```

Configure streaming database source; be sure to provide an appropriate value
for ``<KineticaHostName/IP>``, as well as ``<Username>`` & ``<Password>``, if
the database is configured to require authentication:

```scala
val host = "<KineticaHostName/IP>"
val username = "<Username>"
val password = "<Password>"
val url = s"http://${host}:9191"
val options = Map(
   "database.url" -> url,
   "database.jdbc_url" -> s"jdbc:kinetica://${host}:9191",
   "database.stream_url" -> s"tcp://${host}:9002",
   "database.username" -> username,
   "database.password" -> password,
   "table.name" -> "airline_in",
   "table.create" -> "true",
   "table.is_replicated" -> "false",
   "table.map_columns_by_name" -> "false"
)

val loaderConfig = new LoaderParams(spark.sparkContext, options)
```

Initialize the streaming source table:

```scala
val df = spark.read.
         format("csv").
         option("header", "true").
         option("inferSchema", "true").
         option("delimiter", ",").
         csv("/opt/gpudb/connectors/spark/2008.csv")

df.limit(10).write.format("com.kinetica.spark").options(options).save()
```

Get *Spark* streaming context:

```scala
sc.setLogLevel("ERROR")
val ssc = new StreamingContext(sc, Durations.seconds(5))
```

Establish *table monitor* and *Spark* stream:

```scala
val receiver: GPUdbReceiver = new GPUdbReceiver(loaderConfig);
val inStream: ReceiverInputDStream[AvroWrapper] = ssc.receiverStream(receiver)
inStream.print
ssc.start
```

Once the *table monitor* & *DStream* are established, streaming inserts will
continuously be routed to *Spark* for processing and new records will be
output to the *Spark* console.  Verify that polling of the stream is
occurring at regular intervals and printing out similar text to this:

    -------------------------------------------
    Time: 1530503165000 ms
    -------------------------------------------

At this point, records can be inserted into the ``airline_in`` table at any
time with the following command (press ``ENTER`` at any time to get a
``scala>`` prompt):

```scala
df.limit(10).write.format("com.kinetica.spark").options(options).save()
```

Each time this command is given, a short loading sequence should occur,
followed by a write summary that can be verified to look like this:

    Total rows = 10
    Converted rows = 10
    Columns failed conversion = 10

After each data load, the stream will receive the inserted records and write
them to the *Spark* console:

    {"Year": 2008, "Month": 1, "DayofMonth": 3, "DayOfWeek": 4, "DepTime": "2003", "CRSDepTime": 1955, "ArrTime": "2211", "CRSArrTime": 2225, "UniqueCarrier": "WN", "FlightNum": 335, "TailNum": "N712SW", "ActualElapsedTime": "128", "CRSElapsedTime": "150", "AirTime": "116", "ArrDelay": "-14", "DepDelay": "8", "Origin": "IAD", "Dest": "TPA", "Distance": 810, "TaxiIn": "4", "TaxiOut": "8", "Cancelled": 0, "CancellationCode": null, "Diverted": 0, "CarrierDelay": "NA", "WeatherDelay": "NA", "NASDelay": "NA", "SecurityDelay": "NA", "LateAircraftDelay": "NA"}
    {"Year": 2008, "Month": 1, "DayofMonth": 3, "DayOfWeek": 4, "DepTime": "754", "CRSDepTime": 735, "ArrTime": "1002", "CRSArrTime": 1000, "UniqueCarrier": "WN", "FlightNum": 3231, "TailNum": "N772SW", "ActualElapsedTime": "128", "CRSElapsedTime": "145", "AirTime": "113", "ArrDelay": "2", "DepDelay": "19", "Origin": "IAD", "Dest": "TPA", "Distance": 810, "TaxiIn": "5", "TaxiOut": "10", "Cancelled": 0, "CancellationCode": null, "Diverted": 0, "CarrierDelay": "NA", "WeatherDelay": "NA", "NASDelay": "NA", "SecurityDelay": "NA", "LateAircraftDelay": "NA"}
    {"Year": 2008, "Month": 1, "DayofMonth": 3, "DayOfWeek": 4, "DepTime": "628", "CRSDepTime": 620, "ArrTime": "804", "CRSArrTime": 750, "UniqueCarrier": "WN", "FlightNum": 448, "TailNum": "N428WN", "ActualElapsedTime": "96", "CRSElapsedTime": "90", "AirTime": "76", "ArrDelay": "14", "DepDelay": "8", "Origin": "IND", "Dest": "BWI", "Distance": 515, "TaxiIn": "3", "TaxiOut": "17", "Cancelled": 0, "CancellationCode": null, "Diverted": 0, "CarrierDelay": "NA", "WeatherDelay": "NA", "NASDelay": "NA", "SecurityDelay": "NA", "LateAircraftDelay": "NA"}
    {"Year": 2008, "Month": 1, "DayofMonth": 3, "DayOfWeek": 4, "DepTime": "926", "CRSDepTime": 930, "ArrTime": "1054", "CRSArrTime": 1100, "UniqueCarrier": "WN", "FlightNum": 1746, "TailNum": "N612SW", "ActualElapsedTime": "88", "CRSElapsedTime": "90", "AirTime": "78", "ArrDelay": "-6", "DepDelay": "-4", "Origin": "IND", "Dest": "BWI", "Distance": 515, "TaxiIn": "3", "TaxiOut": "7", "Cancelled": 0, "CancellationCode": null, "Diverted": 0, "CarrierDelay": "NA", "WeatherDelay": "NA", "NASDelay": "NA", "SecurityDelay": "NA", "LateAircraftDelay": "NA"}
    {"Year": 2008, "Month": 1, "DayofMonth": 3, "DayOfWeek": 4, "DepTime": "1829", "CRSDepTime": 1755, "ArrTime": "1959", "CRSArrTime": 1925, "UniqueCarrier": "WN", "FlightNum": 3920, "TailNum": "N464WN", "ActualElapsedTime": "90", "CRSElapsedTime": "90", "AirTime": "77", "ArrDelay": "34", "DepDelay": "34", "Origin": "IND", "Dest": "BWI", "Distance": 515, "TaxiIn": "3", "TaxiOut": "10", "Cancelled": 0, "CancellationCode": null, "Diverted": 0, "CarrierDelay": "2", "WeatherDelay": "0", "NASDelay": "0", "SecurityDelay": "0", "LateAircraftDelay": "32"}
    {"Year": 2008, "Month": 1, "DayofMonth": 3, "DayOfWeek": 4, "DepTime": "1940", "CRSDepTime": 1915, "ArrTime": "2121", "CRSArrTime": 2110, "UniqueCarrier": "WN", "FlightNum": 378, "TailNum": "N726SW", "ActualElapsedTime": "101", "CRSElapsedTime": "115", "AirTime": "87", "ArrDelay": "11", "DepDelay": "25", "Origin": "IND", "Dest": "JAX", "Distance": 688, "TaxiIn": "4", "TaxiOut": "10", "Cancelled": 0, "CancellationCode": null, "Diverted": 0, "CarrierDelay": "NA", "WeatherDelay": "NA", "NASDelay": "NA", "SecurityDelay": "NA", "LateAircraftDelay": "NA"}
    {"Year": 2008, "Month": 1, "DayofMonth": 3, "DayOfWeek": 4, "DepTime": "1937", "CRSDepTime": 1830, "ArrTime": "2037", "CRSArrTime": 1940, "UniqueCarrier": "WN", "FlightNum": 509, "TailNum": "N763SW", "ActualElapsedTime": "240", "CRSElapsedTime": "250", "AirTime": "230", "ArrDelay": "57", "DepDelay": "67", "Origin": "IND", "Dest": "LAS", "Distance": 1591, "TaxiIn": "3", "TaxiOut": "7", "Cancelled": 0, "CancellationCode": null, "Diverted": 0, "CarrierDelay": "10", "WeatherDelay": "0", "NASDelay": "0", "SecurityDelay": "0", "LateAircraftDelay": "47"}
    {"Year": 2008, "Month": 1, "DayofMonth": 3, "DayOfWeek": 4, "DepTime": "1039", "CRSDepTime": 1040, "ArrTime": "1132", "CRSArrTime": 1150, "UniqueCarrier": "WN", "FlightNum": 535, "TailNum": "N428WN", "ActualElapsedTime": "233", "CRSElapsedTime": "250", "AirTime": "219", "ArrDelay": "-18", "DepDelay": "-1", "Origin": "IND", "Dest": "LAS", "Distance": 1591, "TaxiIn": "7", "TaxiOut": "7", "Cancelled": 0, "CancellationCode": null, "Diverted": 0, "CarrierDelay": "NA", "WeatherDelay": "NA", "NASDelay": "NA", "SecurityDelay": "NA", "LateAircraftDelay": "NA"}
    {"Year": 2008, "Month": 1, "DayofMonth": 3, "DayOfWeek": 4, "DepTime": "617", "CRSDepTime": 615, "ArrTime": "652", "CRSArrTime": 650, "UniqueCarrier": "WN", "FlightNum": 11, "TailNum": "N689SW", "ActualElapsedTime": "95", "CRSElapsedTime": "95", "AirTime": "70", "ArrDelay": "2", "DepDelay": "2", "Origin": "IND", "Dest": "MCI", "Distance": 451, "TaxiIn": "6", "TaxiOut": "19", "Cancelled": 0, "CancellationCode": null, "Diverted": 0, "CarrierDelay": "NA", "WeatherDelay": "NA", "NASDelay": "NA", "SecurityDelay": "NA", "LateAircraftDelay": "NA"}
    {"Year": 2008, "Month": 1, "DayofMonth": 3, "DayOfWeek": 4, "DepTime": "1620", "CRSDepTime": 1620, "ArrTime": "1639", "CRSArrTime": 1655, "UniqueCarrier": "WN", "FlightNum": 810, "TailNum": "N648SW", "ActualElapsedTime": "79", "CRSElapsedTime": "95", "AirTime": "70", "ArrDelay": "-16", "DepDelay": "0", "Origin": "IND", "Dest": "MCI", "Distance": 451, "TaxiIn": "3", "TaxiOut": "6", "Cancelled": 0, "CancellationCode": null, "Diverted": 0, "CarrierDelay": "NA", "WeatherDelay": "NA", "NASDelay": "NA", "SecurityDelay": "NA", "LateAircraftDelay": "NA"}

The test JAR, ``kinetica-spark-7.1.<X>.<Y>-tests.jar``, created in the
*Build & Install* section, can be used to run a streaming example.  This command
assumes that the test JAR is also under ``/opt/gpudb/connectors/spark`` on the
*Spark* master node; be sure to provide appropriate values for
``<SparkMasterHostName/IP>``, ``<SparkMasterPort>``, &
``<KineticaHostName/IP>``, as well as ``<Username>`` & ``<Password>``, if
applicable:

```shell
$ spark-submit \
    --master "spark://<SparkMasterHostName/IP>:<SparkMasterPort>" \
    --class "com.kinetica.spark.streaming.StreamExample" \
    --jars /opt/gpudb/connectors/spark/kinetica-spark-7.1.*-tests.jar \
    /opt/gpudb/connectors/spark/kinetica-spark-7.1.*-jar-with-dependencies.jar \
    <KineticaHostName/IP> airline_in airline_out 1000 <Username> <Password>
```

This example will continuously load data into the ``airline_in`` table and
stream the loaded data into both 1) another table named ``airline_out`` and 2)
to a set of files under a directory named ``StreamExample.out`` in the directory
where *Spark* was launched.

**NOTE:**  This test via ``spark-submit`` relies on the ``airline_in`` table
having been created via ``spark-shell`` in the manual *Spark* streaming
example above.


### Property Reference

**Important:**  These properties pertain to deprecated interfaces.  See note at
                [top](#kinetica-spark-guide).

This section describes properties used to configure the connector.  Many
properties are applicable to both connector modes; exceptions will be noted.


#### Connection Properties

The following properties control the authentication & connection to *Kinetica*.

| Property Name                      | Default     | Description
| :---                               | :---        | :---
| ``database.url``                   | *<none>*    | URL of *Kinetica* instance (http or https)
| ``database.jdbc_url``              | *<none>*    | JDBC URL of the *Kinetica ODBC Server*  **Ingest/Egress Processor Only**.  When using SSL, use the `URL=` option to pass in the full URL.  E.g. ``database.jdbc_url="jdbc:kinetica://;URL=https://localhost:8082/gpudb-0"``.
| ``database.primary_url``           | *<none>*    | URL of the primary/active *Kinetica Server* cluster  **Ingest/Egress Processor Only**
| ``database.stream_url``            | *<none>*    | ZMQ URL of the *Kinetica* table monitor  **Streaming Processor Only**
| ``database.username``              | *<none>*    | *Kinetica* login username
| ``database.password``              | *<none>*    | *Kinetica* login password
| ``database.retry_count``           | ``0``       | Connection retry count
| ``database.timeout_ms``            | ``1800000`` | Connection timeout, in milliseconds (default is 30 minutes)
| ``egress.offset``                  | ``0``       | A positive integer indicating the number of initial results to skip before returning records. If the offset is greater than the table size, an empty dataframe will be returned  **Egress Processor Only**
| ``egress.limit``                   | *<none>*    | A positive integer indicating the total number of records to request. The default value will request all records in the table  **Egress Processor Only**
| ``egress.batch_size``              | ``10000``   | A positive integer indicating the size to use for fetching batches of records (multiple batches can be used for a given ``offset`` and ``limit`` combination, if the latter two are given).  **Egress Processor Only**
| ``ingester.analyze_data_only``     | ``false``   | When ``true``, will analyze the ingest data set, determining the types & sizes of columns necessary to hold the ingest data, and will output the derived schema as a ``CREATE TABLE statement`` (at the INFO log level).  **NOTE:** If this parameter is set to ``true``, all others will be ignored.  **Ingest Processor Only**
| ``ingester.batch_size``            | ``10000``   | Batch size for bulk inserter
| ``ingester.fail_on_errors``        | ``false``   | Fail on errors when ingesting data; default behavior is to log warnings and ignore the bad row
| ``ingester.flatten_source_schema`` | ``false``   | When ``true``, converts the following complex source data structures into single-table representations:  *struct*, *array*, & *map*.  See [Complex Data Types](#complex-data-types) for details.  **Ingest Processor Only**
| ``ingester.ip_regex``              | *<none>*    | Regular expression to use in selecting *Kinetica* worker node IP addresses (e.g., ``172.*``) that are accessible by the connector, for multi-head ingest  **Ingest Processor Only**
| ``ingester.multi_head``            | ``true``    | Enable multi-head ingestion
| ``ingester.num_threads``           | ``4``       | Number of threads for bulk inserter
| ``ingester.use_snappy``            | ``false``   | Use *snappy* compression during ingestion  **Ingest Processor Only**
| ``ingester.use_timezone``          | *<none>*    | Use the given timezone when ingesting any date/time/datetime data.  By default, the system timezone will be used.  Allowed formats are standard timezone formats; e.g. ``America/Pacific``, ``EDT``, ``GMT+02:00``, ``GMT-0730``.  Local date/time will not be affected by this setting; only timestamps with a specified offset will be interpreted and saved in the given timezone.  For example, if ``GMT-0500`` is the time zone, and the timestamp value is ``2019-07-21 12:34:56+02:00``, it will be stored in the database as ``2019-07-21 05:34:56``.
| ``spark.datasource_api_version``   | ``v1``      | Which Spark DataSource API to use (accepted values: ``v1`` and ``v2``)  **Data Loader Only**
| ``spark.num_partitions``           | ``4``       | Number of *Spark* partitions to use for extracting data  **Egress Processor Only**
| ``spark.rows_per_partition``       | *<none>*    | Number of records per partition *Spark* should segment data into before loading into *Kinetica*; if not specified, *Spark* will use the same number of partitions it used to retrieve the source data  **Data Loader Only**


The following apply for the *Data Loader* if SSL is used. A keystore or
truststore can be specified to override the default from the JVM.

| Property Name               | Default   | Description
| :---                        | :---      | :---
| ``ssl.keystore_p12``        | *<none>*  | PKCS#12 key store--only for 2-way SSL
| ``ssl.keystore_password``   | *<none>*  | Key store password
| ``ssl.truststore_jks``      | *<none>*  | Java trust store for CA certificate check for the HTTPD server.  If not provided, then the Kinetica server's certificate will not be verified.  To allow for a self-signed certificate, omit this option.
| ``ssl.truststore_password`` | *<none>*  | Java trust store password

##### Data Source/Target Properties

The following properties govern the *Kinetica* table being accessed, as well as
the access mechanism.

| Property Name                   | Default   | Description
| :---                            | :---      | :---
| ``table.create``                | ``false`` | Automatically create table if missing
| ``table.is_replicated``         | ``false`` | Whether the target table is replicated or not  **Ingest Processor Only**
| ``table.name``                  | *<none>*  | *Kinetica* table to access
| ``table.name_contains_schema``  | ``true``  | Indicates that a schema name should be extracted from the ``table.name``, if one is given (separated by periods).  Any additional periods will remain in the table name.
| ``table.truncate``              | ``false`` | Truncate table if it exists
| ``table.truncate_to_size``      | ``false`` | Truncate strings when inserting into charN columns
| ``table.update_on_existing_pk`` | ``false`` | If the target table, ``table.name``, has a primary key, update records in it with matching primary key values from records being ingested
| ``table.use_templates``         | ``false`` | Enable *template tables*; see [Template Tables](#template-tables) section for details  **Data Loader Only**

For the *Data Loader*, the following properties specify the data source &
format.

| Property Name          | Default   | Description
| :---                   | :---      | :---
| ``source.csv_header``  | ``false`` | If format is CSV, whether the file has column headers or not; if ``true``, the column headers will be used as column names in creating the target table if it doesn't exist and mapping source fields to target columns if the table does exist.  If ``false``, columns will be mapped by position.
| ``source.data_format`` | *<none>*  | Indicates the format of the file(s) in ``source.data_path``.  Supported formats include:  ``avro``, ``csv``, ``json``, ``orc``, & ``parquet``
| ``source.data_path``   | *<none>*  | File or directory in Hadoop or the local filesystem containing source data
| ``source.sql_file``    | *<none>*  | File containing a SQL-compliant query to use to retrieve data from *Hive* or *Spark-SQL*

For the *Ingest/Egress Processor*, the following properties govern
evolving/drifting schemas.

| Property Name                 | Default   | Description
| :---                          | :---      | :---
| ``table.append_new_columns``  | ``false`` | Whether the *Ingest Processor* should append columns from the source that don't exist in the target table to the target table
| ``table.map_columns_by_name`` | ``true``  | Whether the *Ingest Processor* should map ``DataFrame`` columns by name or position; if ``true``, columns in the ``DataFrame`` will be mapped in case-sensitive fashion to the target table; if ``false``, ``DataFrame`` columns will be mapped by position within the ``DataFrame`` to position within the target table
