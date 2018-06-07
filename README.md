# Kinetica Spark Connector

This project contains the **6.2** version of the **Kinetica Spark Connector** for integration of Kinetica with Spark using the Spark DataSource API.

## Overview

This documentation is broken into the following sections:

* [Connector API](#connector-api): Call the API directly for Kinetica ingest and egress.
    * Use with Scala, `spark-shell`,  PySpark, or custom applications.
    * Create DataFrames with the API for ergress to Kinetica tables.
    * Save DataFrames to Kinetica with the `com.kinetica.spark` format.
* [Command-line Loader](#spark-loader): Configuration file driven ingest application.
    * Configuration can be done entirely in properties files with no coding required.
    * Ingest from Spark supported formats and sources into Kinetica.
    * Loader can launched with 'spark-submit' or a custome script.
    * Ingest from a Hive SQL query.
* [Spark Streaming](#kinetica-spark-streaming): Low-latency streaming to and from Kinetica tables.
    * Start a streaming service to recieve data from Kinetica 
    * Work on those batches of streams within Spark.
    * The non-streaming portions are mostly Scala with some Java.
    * The streaming portion is mostly Java.

## External Resources

* [Kinetica Spark Guide](http://www.gpudb.com/docs/6.2/connectors/spark_guide.html)
* [Kinetica Documentation Top](http://www.kinetica.com/docs/6.2/index.html)
* [Kinetica Spark Docker Image](https://bitbucket.org/gisfederal/gpudb-docker/src/HEAD/spark-connector/?at=master)
* [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

## Build & Install

You can clone the git repository with the following command:

```bash
$ git clone https://github.com/kineticadb/kinetica-connector-spark.git
```

Important notes:
* Ensure that the connector version matches your Kinetica server version. 
* Use the appropriate branch of this repository based on your Kinetica server version (e.g `v6.2.0`)
* Use the shaded AVRO classes. See `pom.xml` for details.

The connector jar can be built with *Maven* as follows:

```bash
$ cd kinetica-connector-spark
$ mvn clean package
```

This should produce the connector jar under the `target` directory:

```bash
target/kinetica-spark-6.2.1-jar-with-dependencies.jar
```

This jar should be made available to the *Spark* cluster.

## Connector API

The **Spark Connector API** provides easy integration of **Spark v2.x** with **Kinetica** for rapid data
ingestion. All interactions with the connector are through the API exposed inside the connector
jar. One can use it via the spark shell and also in scripts.

Features of the *Kinetica Spark Connector*:

* Accepts *Spark DataFrames*
* Creates *Kinetica* tables from *Spark DataFrames*
    * Optimizes datatype conversions between *Spark* and *Kinetica*
    * Automatic right-sizing of string and numeric fields
* Massive parallel ingestion
    * 3+ billion inserts per minute
* Able to create spark DataFrame from Kinetica tables
    * Able to prune columns and push down filters

### Architecture

The connector accepts *Spark* `DataFrame` and `LoaderParams` objects.  The `LoaderParams`
object holds all necessary configuration to interface with *Kinetica*.

The connector is case sensitive when it maps dataset column names to *Kinetica* table column names.
This is an existing limitation with *Spark* `row.AS`. Another option exposed via connector is to
map data to columns based on position.  If this is used, the number & order of columns must match
between the dataset and table.

The `SparkKineticaTableBuilder` API will extract the data types from a *Spark* `DataFrame` and
construct a table in *Kinetica* with the corresponding schema. The following *Spark* datatypes are
supported:

* `ByteType`
* `ShortType`
* `IntegerType`
* `LongType`
* `FloatType`
* `DoubleType`
* `DecimcalType`
* `StringType`
* `BooleanType` (converted to 1 or 0, integer)
* `DateType`
* `TimestampType`

Once the target table has been created, the `SparkKineticaLoader` can be used to load data into
it.

Each *Spark* `DataFrame` partition instantiates a *Kinetica* `BulkInserter`, which is a native
API for MPP ingest.

![Spark Connector Architecture](doc/architecture.png)

### Usage Considerations

* The connector does not perform any ETL transformations. 
* Data types must match between *Spark* and *Kinetica*.
* For row updates, columns not present during update will be set to `null`.
* Each `Dataset` partition should handle no fewer than 1-2 million records * To load multiple
    tables, call `LoaderParams.setTablename()` prior to calling
`SparkKineticaLoader.KineticaWriter()` for each table loaded.
* If LDAP/SSL is enabled, the connection string must point to the SSL URL and a valid certificate
    must be used.

### Examples

#### Ingest with spark-shell

The following example shows how to load data into *Kinetica* via `DataFrame`. It will first read
airline data from CSV into a `DataFrame`, and then load the `DataFrame` into *Kinetica*.  The data
file comes from the 2008 airline data set, available here:

* <http://stat-computing.org/dataexpo/2009/2008.csv.bz2>

This example assumes the `2008.csv` & *Spark* connector jar are located in the
`/opt/gpudb/connectors/spark` directory. Below <sandbox> refers to the directory where you have
the jar files and the data directory containing the `2008.csv` file

1. Launch *Spark Shell*:
```bash
$ ./bin/spark-shell
    --jars <sandbbox>/kinetica-spark-6.2.1-jar-with-dependencies.jar
```
1. Set some values:
```scala
    scala> val dataDir = "<sandbox>/data"
```
1. Create Dataset:
```scala
scala> :paste
// Entering paste mode (ctrl-D to finish)
scala> val userDF = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .csv(s"${dataDir}/2008.csv");
// Exiting paste mode, now interpreting.
```
1. Load data into *Kinetica*:
```scala
scala> var writeToKineticaOpts = Map(
        "kinetica-url" -> "http://gpudb:9191",
        "kinetica-desttablename" -> "airline",
        "kinetica-replicatedtable" -> "false",
        "kinetica-ipregex" -> "",
        "kinetica-batchsize" -> "10000",
        "kinetica-updateonexistingpk" -> "true",
        "kinetica-maptoschema" -> "false",
        "kinetica-numthreads" -> "4",
        "kinetica-createtable" -> "true",
        "kinetica-jdbcurl" -> "jdbc:simba://gpudb:9292;ParentSet=test",
        "kinetica-username" -> "",
        "kinetica-password" -> "");
scala> userDF.write.format("com.kinetica.spark").options(writeToKineticaOpts).save()
```

After the write is finished you should find a table called `airline` in Kinetica.


#### Ingest with compiled Scala and spark-submit 

The above steps performed with `spark-shell` can also be done with the help of a driver code found in
the test jar. The execution will look as follows. This test jar should be available from the machine
executing the following spark-submit command.

The following examples can be used with `spark-submit`:

| File Name                                                     | Description |
| :---                                                          | :---  |
| `src/test/scala/com/kinetica/spark/KineticaIngestTest.scala`  | Scala ingest example. |
| `src/test/scala/com/kinetica/spark/KineticaEgressTest.scala`  | Scala egress example. |

Please note that if a multi-node Spark cluster is used and the job is submitted to the cluster, it
is imperative that the data files is accessible from all the Spark nodes. This is automatic if Spark
is riding on a HDFS file system. But if that is not the case, for testing purposes, one can copy the
data file across all the nodes.

```bash
./bin/spark-submit
    --master "spark://<spark master host>:<spark port>"
    --class "com.kinetica.spark.KineticaIngestTest"
    --jars <sandbox>/kinetica-spark-6.2.1-tests.jar 
    <sandbox>/kinetica-spark-6.2.1-jar-with-dependencies.jar
    <sandbox>/data/2008.csv
```

#### Egress with spark-shell

```bash
$ ./bin/spark-shell
    --jars <sandbox>/kinetica-spark-6.2.1-jar-with-dependencies.jar
```

```scala
scala> val kineticaOptions = Map(
    "kinetica-jdbcurl" -> "jdbc:simba://gpudb:9292;ParentSet=test",
    "kinetica-username" -> "",
    "kinetica-password" -> "",
    "kinetica-desttablename" -> "airline",
    "connector-numparitions" -> "4")

scala> val sqlContext = spark.sqlContext
scala> val productdf = sqlContext.read.format("com.kinetica.spark").options(kineticaOptions).load()

scala> val df = productdf.filter("DayOfMonth > 12")
scala> println(df.count)
```

When data is extracted from Kinetica and Dataframes created on Spark for further processing, the
filters can be pushed down to Kinetica. Let us condider the  above code. The intention is to create
a DataFrame on Spark based on the table `airline` but fetch only the rows which satisfies the filter
`DayOfMonth > 15`. Firstly, the connector fetches counts of rows from the table (`airline`) for the
particualr filter. The query looks like this

```sql
select count(*) from airline WHERE DayofMonth is not null AND DayofMonth > 12
```

This gives a count of rows (say X) back. These X rows (could be 0 rows) are then fetched by the
specified number of partitions. As an example, say the count query returned a total of 15 rows (i.e.
a total of 15 rows returned true for the filter), and user specified 4 partitions, then we would be
executing 4 separate queries into Kinetica will be executed to fetch the data. They are:

```sql
SELECT CINT,CSTRING,CDOUBLE
FROM ALLTYPE
WHERE CDOUBLE is not null
    AND CINT is not null AND CDOUBLE >= 10.5
    AND CINT >= 11
LIMIT 0,4
```

```sql
SELECT CINT,CSTRING,CDOUBLE
FROM ALLTYPE
WHERE CDOUBLE is not null
    AND CINT is not null AND CDOUBLE >= 10.5
    AND CINT >= 11
LIMIT 4,4
```

```sql
SELECT CINT,CSTRING,CDOUBLE
FROM ALLTYPE
WHERE CDOUBLE is not null
    AND CINT is not null AND CDOUBLE >= 10.5
    AND CINT >= 11
LIMIT 8,4
```

```sql
SELECT CINT,CSTRING,CDOUBLE
FROM ALLTYPE
WHERE CDOUBLE is not null
    AND CINT is not null AND CDOUBLE >= 10.5
    AND CINT >= 11
LIMIT 12,4
```

As we can see the filter is being pushed down into Kinetica and is evaluated there. It is worth
noting that the following will not be pushed down and will be executed in Spark only after data is
fetched from Kinetica:

```scala
println(df.groupBy("FlightNum").sum("DayOfWeek").show())
```

#### Ingest CSV with PySpark shell

Below is a working session snippet with pyspark:

```bash
$ ./pyspark 
    --jars <sandbox>/kinetica-connector-spark/target/kinetica-spark-6.2.1-jar-with-dependencies.jar

Using Python version 2.7.5 (default, Apr 11 2018 07:36:10)
SparkSession available as 'spark'.

>>> from pyspark.sql import SQLContext

>>> sqlContext = SQLContext(sc)

>>> df = sqlContext.read.load('/home/shouvik/rough/2008.csv',
    format='com.databricks.spark.csv',
    header='true',
    inferSchema='true',
    delimeter='|')

>>> df.write.format("com.kinetica.spark")
    .option("kinetica-url","http://gpudb:9191")
    .option("kinetica-desttablename","airline")
    .option("kinetica-replicatedtable" ,"false")
    .option("kinetica-maptoschema","false")
    .option("kinetica-createtable","true")
    .option("kinetica-jdbcurl","jdbc:simba://gpudb:9292;ParentSet=test")
    .save()
```

Your data from the CSV file now should be present in a Kinetica table called `airline`.

#### Egress AVRO with scala script and spark-submit

This example uses a pyspark script to egress the `flights` table created from the previous example. It consists of the following files.

| File Name                          | Description |
| :---                               | :---  |
| `scripts/shell/egress_avro.scala`  | Scala script. |
| `scripts/shell/egress_avro.sh`     | Launcher script. |

Execute the `egress_avro.sh` to run `egress_avro.scala` with `spark-shell`. 

```scala
/opt/spark-test/kinetica-spark-6.2.1/scripts/shell $ ./egress_avro.sh
+ spark-shell --jars ../../target/kinetica-spark-6.2.1-jar-with-dependencies.jar --packages com.databricks:spark-avro_2.11:4.0.0

scala> val KineticaOptions = Map(
     |     "kinetica-jdbcurl" -> "jdbc:simba://gpudb:9292;ParentSet=test",
     |     "kinetica-username" -> "",
     |     "kinetica-password" -> "",
     |     "kinetica-desttablename" -> TableName,
     |     "connector-numparitions" -> "4")
KineticaOptions: scala.collection.immutable.Map[String,String] = Map(kinetica-password -> "", connector-numparitions -> 4, kinetica-desttablename -> flights, kinetica-jdbcurl -> jdbc:simba://gpudb:9292, kinetica-username -> "")

scala> val tableDF = spark.read.format("com.kinetica.spark").options(KineticaOptions).load()
tableDF: org.apache.spark.sql.DataFrame = [TRACKID: string, heading: string ... 12 more fields]

scala> println(s"Writing output lines. (rows = ${tableDF.count()})")
Writing output lines. (rows = 100)

scala> tableDF.coalesce(1).write.mode("overwrite").avro("output_avro")

scala> :quit
```

#### Ingest with PySpark and spark-submit

This example uses a pyspark script to ingest the `flights` table. It consists of the following files.

| File Name                       | Description |
| :---                            | :---  |
| `scripts/python/ingest_csv.py`  | PySpark scrpt. Edit this file with your connection information. |
| `scripts/python/ingest_csv.sh`  | Launcher script. |
| `scripts/data/flights.csv`      | Data file to load. |

When you run `ingest_csv.sh` it will pass `ingest_csv.py` to `spark-submit` and load the flights table.

```
/opt/spark-test/kinetica-spark-6.2.1/scripts/python $ ./ingest_csv.sh
+ spark-submit --deploy-mode client --master 'local[8]' --jars ../../target/kinetica-spark-6.2.1-jar-with-dependencies.jar ingest_csv.py
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
18/05/25 16:41:40 INFO SparkContext: Running Spark version 2.2.0
18/05/25 16:41:41 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/05/25 16:41:41 INFO SparkContext: Submitted application: ingest_csv
[...]
```


## Spark Loader

This feature facilitates fetching data from a SQL select statement or data file and inserting
 the results into a Kinetica table.  Highlights include:

* **Data Sources:** Supports input from SQL, AVRO, or common Spark formats.
* **Repeatability:** Configuration files are used for setup of a repeatable and automated job.
* **No Coding:** Other than the input SQL statement no Java or Scala coding is required.
* **Scalable:** It has all the runtime options of a Spark job and can be submitted locally, on a Spark
cluster, or as a YARN job.
* **Type Conversion:** Spark types are automatically mapped to Kinetica types when tables are automatically
created or when loading into existing tables.
* **Schema Management**: Tables can be automatically created or truncated. Schema can be version controlled
with *template tables*.
* **Schema merging**: Differences between Spark dataframe and destination table are reconciled.

### Property Reference

#### Overview

This section describes properties used to configure the loader. They can come from one of the following
locations:

1. Top-level file: The properties file you pass in on the command line.
2. Included file: A properties file included from the top level properties file.
3. Command line: A command line argument that overrides values in the properties files.

#### Connection Properties

Before launching the job you must specify the following connection information:

| Property Name             | Default   | Description   |
| :---                      | :---      | :---          |
| `kinetica.url`            | <none>    | URL of Kinetica instance. (http or https) |
| `kinetica.username`       | empty     | Optional username |
| `kinetica.password`       | empty     | Optional password |
| `kinetica.multihead`      | false     | Enable multi-head ingest. This must be false for replicated tables. |
| `kinetica.numthreads`     | 4         | Number of threads for bulk inserter  |
| `kinetica.batchsize`      | 1000      | Batch size for bulk inserter |
| `kinetica.timeoutms`      | 10000     | Kinetica connection timeout in MS  |

#### Data Source Properties

For the data source you must specify either a SQL file or a data file path/format but not both.

1. SQL File: The SQL statement is executed and data is retrieved as indicated by the Hive metastore.
2. Data Path: Indicates that the data should be retrieved directly from Hadoop.

| Property Name             | Default   | Description   |
| :---                      | :---      | :---          |
| `connector.sqlfile`       | <none>    | File containing SQL that will be used to retrieve data from Hive or Spark-SQL |
| `connector.datapath`      | <none>    | File or directory in Hadoop or the local filesystem |
| `connector.dataformat`    | <none>    | Indicates the format of the file or files in `data-path` |

Note: Examples of supported formats are: `avro`, `json`, `orc`, `parquet`, `csv`

#### Data Target Properties

The following properties control the handling of ingest to the target table.

| Property Name                 | Default   | Description   |
| :---                          | :---      | :---          |
| `kinetica.desttablename`      | <none>    | Target Kinetica table. If this table does not exist it will be created. |
| `kinetica.createtable`        | false     | Automatically create table if missing |
| `kinetica.truncatetable`      | false     | Truncate table if it exists |
| `kinetica.usetemplates`       | false     | See **Template Tables** section |
| `kinetica.updateonexistingpk` | false     | If the table has a PK then replace the row if it exists.  |
| `kinetica.partitionrows`      | <none>    | Maximum number of rows in a Spark partition. |

#### SSL Properties

The following apply if SSL is used. You can optionally specify a keystore or truststore that will override
the default from the JVM.

| Property Name                 | Description           |
| :---                          | :---                  |
| `kinetica.truststorejks`      | JKS trust store for CA certificate check. |
| `kinetica.truststorepassword` | Trust store password |
| `kinetica.keystorep12`        | PKCS#12 key store. Only for 2-way SSL |
| `kinetica.keystorepassword`   | Key store password. |
| `kinetica.sslbypasscertcheck` | Indicates if CA certificate check should be skipped. |

An example SSL configuration is available in `gpudb-secure.properties`.

### Usage

You can use the `spark-submit` program to launch the job. The below example will launch the job locally:

```
$ spark-submit \
  --class com.kinetica.spark.SparkKineticaDriver \
  --master local[*] \
  --deploy-mode client \
  --packages com.databricks:spark-avro_2.11:4.0.0 \
  ./kinetica-spark-6.2.1-jar-with-dependencies.jar csv-test.properties
```

More details about submitting Spark jobs are available in [Submitting Spark Applications][SUBMITTING].

[SUBMITTING]: <https://spark.apache.org/docs/latest/submitting-applications.html>

### Template Tables

The template table feature is activated when `loader.use-templates = true`. It provides a method for schema
versioning when tables are created with the loader.

To use this feature you create a template table and collection where the naming follows a specific pattern
derived from the destination table name and collection.

```
collection = <collection>.template
table = <table name>.<version string>
```

When searching for a schema the loader will search for the pattern, sort descending, and use the schema
from the first result to create the destination table.

For example if you have a table `test.avro_test` you could have a set of schema versions as follows:

```
collection = test.template
table = avro_test.20171220
table = avro_test.20180130
table = avro_test.20180230
```

When creating the table `avro_test` it will use the schema from `avro_test.20180230` because it shows up
first in the reverse sort.

### Schema Merging

The Spark Dataframe and Kinetica table schemas could could have different columns or the columns could have
different types. In this situation the loader will apply schema merging rules and build a mapping of
source and destination columns.

The following rules apply when matching columns:

* Columns in the source and destination schemas are matched by name as case sensitive.
* If a column does not exist in both schemas it is ignored.

If the column being mapped is numeric then a [Widening Primitive Conversion][WIDENING] is applied if
necessary. When converting types the mapper will use the associated Java type of each column for
comparison. The following conversions are permitted:

| Source column | Destination column    |
| ---           | ---                   |
| Integer       | Long                  |
| Float         | Double                |
| Boolean       | Integer               |
| Date          | Long                  |

The following conditions will cause the mapping to fail. These conditions are detected during setup and
no workers are launched.

* If a column is not mapped and is null in the destination table.
* If a column is mapped and would result in a [Narrowing Primitive Conversion][NARROWING].

[NARROWING]: <https://docs.oracle.com/javase/specs/jls/se7/html/jls-5.html#jls-5.1.3>
[WIDENING]: <https://docs.oracle.com/javase/specs/jls/se7/html/jls-5.html#jls-5.1.2>

### Examples

The distribution zip contains example jobs for Avro and CSV data sets. They contain the following files.

| File Name                             | Description |
| :---                                  | :---  |
| `scripts/loader/gpudb.properties`     | Common parameter file. |
| `scripts/loader/run-spark-loader.sh`  | Launcher script |
| `scripts/loader/csv-test.properties`  | Top-level parameter file. |
| `scripts/loader/csv-test`             | CSV data containing 50 test records. |
| `scripts/loader/avro-test.properties` | Top-level parameter file. |
| `scripts/loader/avro-test`            | Avro data containing 1000 test records. |
| `scripts/loader/run-spark-loader.sh`  | Launcher script |
| `src/test/scala/com/kinetica/spark/SparkKineticaDriver.scala` | Loader scripting example. |

To run an example configure the `gpudb.properties` with your connection information and exececute
`run-spark-loader.sh` as shown below.

```sh
/opt/spark-test/kinetica-spark-6.2.1/scripts/loader$ ./run-spark-loader.sh csv-test.properties
Using master: local[8]
+ spark-submit --class com.kinetica.spark.SparkKineticaDriver
    --master 'local[8]' --deploy-mode client
    --packages com.databricks:spark-avro_2.11:4.0.0
    --driver-java-options -Dlog4j.configuration=file:/opt/spark-test/kinetica-spark-6.2.1/scripts/loader/log4j.properties
    ../../target/kinetica-spark-6.2.1-jar-with-dependencies.jar csv-test.properties
INFO  com.kin.spa.SparkKineticaDriver (SparkKineticaDriver.scala:112) - Reading properties from file: csv-test.properties
INFO  org.apa.spa.SparkContext (Logging.scala:54) - Running Spark version 2.2.0
[...]
INFO  org.apa.spa.SparkContext (Logging.scala:54) - Successfully stopped SparkContext
```

## Kinetica Spark Streaming

The following guide provides step by step instructions to get started integrating *Kinetica* with
*Spark Streaming*.

This project is aimed to make *Kinetica Spark* accessible, meaning an `RDD` or `DStream` can be
generated from a *Kinetica* table or can be saved to a *Kinetica* table.

#### Connector Classes

The three connector classes that integrate *GPUdb* with *Spark* are:

`com.gpudb.spark.input`:

* `GPUdbReader` - Reads data from a table into an `RDD`
* `GPUdbReceiver` - A *Spark* streaming `Receiver` that receives data from a *GPUdb* table monitor stream

`com.gpudb.spark.output`:

* `GPUdbWriter` - Writes data from an `RDD` or `DStream` into *GPUdb*

### Configuration

The *GPUdb Spark* connector uses the *Spark* configuration to pass *GPUdb* instance information to
the *Spark* workers. Ensure that the following properties are set in the *Spark* configuration
(`SparkConf`) of the *Spark* context (`JavaSparkContext`) when using each of the following connector
interfaces:

`GPUdbReader`

* `gpudb.host` - The hostname or IP address of the *GPUdb* instance
* `gpudb.port` - The port number on which the *GPUdb* service is listening
* `gpudb.threads` - The number of threads *GPUdb* should use
* `gpudb.table` - The name of the *GPUdb* table being accessed
* `gpudb.read.size` - The number of records to read at a time from *GPUdb*

`GPUdbWriter`

* `gpudb.host` - The hostname or IP address of the *GPUdb* instance
* `gpudb.port` - The port number on which the *GPUdb* service is listening
* `gpudb.threads` - The number of threads *GPUdb* should use
* `gpudb.table` - The name of the *GPUdb* table being accessed
* `gpudb.insert.size` - The number of records to queue before inserting into *GPUdb*

### Use Cases

#### Loading Data from GPUdb into a Spark RDD

To read from a *GPUdb* table, create a class that extends `RecordObject` and implements `Serializable`. For example:

```scala
public class PersonRecord extends RecordObject implements Serializable
{
    @RecordObject.Column(order = 0, properties = { ColumnProperty.DATA })
    public long id;

    @RecordObject.Column(order = 1)
    public String name;

    @RecordObject.Column(order = 2, properties = { ColumnProperty.TIMESTAMP })
    public long birthDate;

    public PersonRecord(){}
}
```

Note: The column order specified in the class must correspond to the table schema.

Next, instantiate a `GPUdbReader` for that class and call the `readTable` method with an optional filter `expression`:

```scala
GPUdbReader<PersonRecord> reader = new GPUdbReader<PersonRecord>(sparkConf);
JavaRDD<PersonRecord> rdd = reader.readTable(PersonRecord.class, expression, sparkContext);
```

The `expression` in the `readTable` call is equivalent to a SQL `WHERE` clause.  For details, read
the *Expressions* section of the *Concepts* page.

####  Saving Data from a Spark RDD to GPUdb

Creating a *GPUdb* table::

```scala
GPUdbUtil.createTable(gpudbUrl, collectionName, tableName, PersonRecord.class);
```

Writing to a *GPUdb* table:

```scala
final GPUdbWriter<PersonRecord> writer = new GPUdbWriter<PersonRecord>(sparkConf);
writer.write(rdd);
```

####  Receiving Data from GPUdb into a Spark DStream

The following creates a `DStream` from any new data inserted into the table `tableName`:

```scala
GPUdbReceiver receiver = new GPUdbReceiver(gpudbUrl, gpudbStreamUrl, tableName);
JavaReceiverInputDStream<AvroWrapper> dstream = javaStreamingContext.receiverStream(receiver);
```

Each record in the `DStream` is of type `AvroWrapper`, which is an *Avro* object along with its schema to decode it.

Note:  At this time, only `add` and `bulkadd` functions will trigger the *GPUdb* to publish added
records to *ZMQ* to be received by the *Spark* streaming interface.  New records can also be added
via the *GPUdb* administration page.

####  Saving Data from a Spark DStream to GPUdb

Creating a *GPUdb* table::

```scala
GPUdbUtil.createTable(gpudbUrl, collectionName, tableName, PersonRecord.class);
```

Writing to a *GPUdb* table::

```scala
final GPUdbWriter<PersonRecord> writer = new GPUdbWriter<PersonRecord>(sparkConf);
writer.write(dstream);
```

### Examples

The following example files are provided:

| File Name                                                 | Description |
| :---                                                      | :---  |
| `scripts/stream/streamexmpl.properties`                   | Streaming example configuration. |
| `scripts/stream/streamexmpl.sh`                           | Streaming example launcher. |
| `src/test/java/com/kinetica/spark/StreamExample.java`     | Reading & writing *GPUdb* data via *Spark* using a `DStream` |

These examples assume launching will be done on a *Spark* server using `/bin/spark-submit`.  The
`streamexmpl.sh` script can run each example with minimal configuration via the
`streamexmpl.properites` file.

The `gpudb.host` property in `streamexmpl.properties` should be modified to be the name of the *GPUdb* host being accessed.

To run the example, issue this command with no parameters to display usage information::

```bash
$ cd scripts/stream
$ ./streamexmpl.sh
```
