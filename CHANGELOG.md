# Kinetica Spark Connector Changelog

## Version 7.1

### Version 7.1.1.0 -- 2021-04-08

#### Changed
-   Adding support for Spark 2.4.5


### Version 7.1.0.0 -- 2020-06-26

#### Deprecated
-   Option `table.name_contains_schema`.
    Kinetica 7.1 has new restrictions on table and schema names preventing
    dot symbol in the name literal. Therefore any single dot in the value of
    Spark connector option `table.name` is parsed as if `table.name` is in
    `SCHEMA_NAME.TABLE_NAME` notation, multiple dots are prohibited, and
    there is no table name referencing ambiguity that has to be resolved with
    a separate option `table.name_contains_schema`.

#### Changed
-   When using templates with Spark connector we recommend to start using `_`
    symbol in place of `.` to comply with Kinetica 7.1 table and schema
    naming restrictions.
-   Non-existent table schema provided as a part of qualified table name would
    be explicitly created before creating the table object.

## Version 7.0

### Version 7.0.7.0 -- 2021-04-02

#### Changed
-   Adding support for Spark 2.4.5


### Version 7.0.6.4 -- 2020-08-17

#### Added
-   Creating two Spark Connector build artifacts; one for regular use
    (`kinetica-spark-7.1.0.0-jar-with-dependencies.jar`) and another
    with shaded `com.typesafe.scalalogging` to be used with production
    level Spark server deployments that could have a version conflict with scalalogging
    dependency (`kinetica-spark-7.1.0.0-jar-with-shaded-scalalogging.jar`).


### Version 7.0.6.3 -- 2020-07-01

#### Fixed
-   Check if Spark connector is running in `--analyze-only` mode
    before conecting to Kinetica instance.
-   SSL connection error when using custom truststore


### Version 7.0.6.2 -- 2020-06-10

#### Fixed
-   Check if table exists before attempting to create Kinetica table.

### Version 7.0.6.1 -- 2020-02-05

#### Added
-   Timestamp to Datetime and Datetime to Timestamp conversion logic.
-   Nanosecond group (up to 9 digits) to Datetime formatter pattern.
-   Improved logging when `ingester.fail_on_errors` configuration
    option is set to `true`.

#### Fixed
-   Enforced saving Spark singleton configuration options without duplication.

### Version 7.0.6.0 -- 2019-12-05

#### Added
-   New egress option to control the batch size:
    - ``egress.batch_size``

#### Changed
-   Egress to use lazy iterators for streaming data into partitions


### Version 7.0.5.2 -- 2019-10-25

#### Changed
-   Java API and JDBC connector version dependency due to significant bugfixes in
    those two packages.


### Version 7.0.5.1 -- 2019-10-23

#### Fixed
-   Issues in egress options offset and limit when they are beyond table sizes.


### Version 7.0.5.0 -- 2019-10-22

#### Added
-   New egress options to allow fetching part of a table instead of the whole table:
    - ``egress.offset``
    - ``egress.limit``


### Version 7.0.4.1 -- 2019-09-23

#### Fixed
-   Egress issue when exporting nulls

### Version 7.0.4.0 -- 2019-09-16

#### Deprecated
-   SSL certification verification bypass flag ``ssl.bypass_cert_check``; the
    connector still accepts this flag, but it is now internally ignored.  The
    bypassing happens automatically if the JKS truststore is not passed via the
    ``ssl.truststore_jks`` flag (which is optional).


### Version 7.0.3.6 -- 2019-08-30

#### Added
-   Added an ingest processor parameter ``database.primary_url`` that allows the
    user to specify primary Kinetica server cluster.


### Version 7.0.3.5 -- 2019-08-22

#### Changed
-   Converted the ingest processor default for parameter ``ingester.multi_head``
    ``true``.


### Version 7.0.3.4 -- 2019-08-09

#### Changed
-   Using the `maven-shade-plugin` for building the package; changes:
    -   Relocated package `com.typesafe.scalalogging` to `com.kinetica.scalalogging`
    -   Minimized the uber jar to reduce size

#### Fixed
-   Users with the proper table read access can now egress data from the table.


### Version 7.0.3.3 -- 2019-08-05

#### Added
-   Support for the `table.truncate_to_size` parameter to the ingester process

#### Fixed
-   Ingestor log to correctly show the number of records converted when
    errors occur upon ingestion.


### Version 7.0.3.2 -- 2019-07-29

#### Added
-   A new configuration property called `ingester.use_timezone` which allows
    the user to set a timezone for parsing date/time/datetime with respect
    to it.  Omitting it uses the system's default timezone.
-   A new configuration property call `ingester.fail_on_errors` which allows
    the ingester process to continue when encountering bad rows.  With the
    default value of `false`, the process will only log warnings and continue.
    With the value of `true`, it will throw an exception on the first
    encountering of an error.

#### Changed
-   When the dataframe to be ingested has a schema but is empty, create
    the table.
-   For egress from Kinetica, changed Kinetica time-type (base string) mapping
    to spark sql StringType--instead of TimestampType--so that times don't get
    prepended with the current date.


#### Fixed
-   Filters for egress are now pushed down to the database correctly.


### Version 7.0.3.1 -- 2019-07-05

#### Changed
-   When the dataframe is empty for ingestion, log a warning instead of
    throwing an exception, and return without attempting ingestion.

#### Fixed
-   Ingestion of string timestamps into long timestamp columns
-   Ingestion of non-float numbers into float columns


### Version 7.0.3.0 -- 2019-05-29

#### Changed
-   Updated the Kinetica Java API version to 7.0.3.0 to take advantage of
    recent changes (support for HA failover for multi-head I/O).

#### Fixed
-   The POM now allows a build even when no git directory exists (to disable
    the git-commit-id plugin).

### Version 7.0.2.2 -- 2019-04-25

#### Changed
-   Kinetica egress to accommodate very large tables (such that partitions
    can have more than Int.MaxValue rows).

### Version 7.0.2.1 -- 2019-04-21

#### Added
-   ScalaTest based tests; compilation now requires -DskipTests to compile
    the package without running tests (which might be necessary if you don't
    have a running Kinetica instance).

#### Fixed
-   Quoting table names for egress from Kinetica


### Version 7.0.2.0 -- 2019-04-10


#### Changed
-   Added quotes around schema/collection name, table name, and column names.
-   Allowing multiple periods in the table name (the first name will still
    be considered to be a schema/collection and table name separator).
-   Added a configuration parameter `table.name_contains_schema` with a default
    value of `true`, which indicates that a schema name should be extracted
    from the existing `table.name` parameter, if given any.


### Version 7.0.0.2 -- 2019-04-01

#### Changed
-   Changed the default timeout to 30 minutes
-   Changed the default retry count to 0

#### Fixed
-   Fixed egress issue related to importing string column type with the
    `datetime` subtype.
-   Fixed decimal ingestion failure (decimal in Kinetica can be mapped to any
    of float, double, int, long, string value based on the scale and precision).



### Version 7.0.0.1 -- 2019-02-13

##### Changed
-   Kinetica egress now uses the native Java client API to fetch records instead
    of utilizing the JDBC connector.

### Version 7.0.0.0 -- 2019-01-31

#### Changed
- Updated the JDBC connector to use the 7.0 JDBC connector


## Version 6.2

### Version 6.2.2.1 -- 2019-02-13

##### Changed
-   Kinetica egress now uses the native Java client API to fetch records instead
    of utilizing the JDBC connector.


### Version 6.2.2

#### Added
-   Added a new package `com.kinetica.spark.datasourcev1` that is the current
    implementation utilizing Spark's DataSource v1 API.  This acts as the default
    package when specifying `com.kinetica.spark`.
-   Added a new package `com.kinetica.spark.datasourcev2` that is utilizes
    Spark's DataSource v2 API (which is still evolving; so you should use v1).  This
    path does NOT alter destination Kinetica tables on the fly.
-   Loader configuration parameter `spark.datasource_api_version`


### Version 6.2.1

#### Changed
-   Refactored code to use Spark's DataSource API
-   Now handling byte array types (Kinetica BYTE datatype)
-   Stringification of some complex datatypes. Creating columns with unrestricted string type
    for those data items.
-   Parameter names now allow embedded periods



### Version 6.2.0

-   Aligned examples to take username & password
-   Added streaming API example
-   Parameter names consolidated between Loader, Ingest/Egress Processor and Streamer
-   Added federated query example
-   Added PySpark example
-   Added support for headers and type inference for CSV files.
