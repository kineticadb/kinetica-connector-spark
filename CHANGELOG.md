# Kinetica Spark Connector Changelog

## Version 6.2

### Version 6.2.2.5 -- 2019-05-29

#### Changed

-   Kinetica egress to accommodate very large tables (such that partitions
    can have more than Int.MaxValue rows).

#### Fixed
-   The POM now allows a build even when no git directory exists (to disable
    the git-commit-id plugin).


### Version 6.2.2.4 -- 2019-04-21

#### Added
-   ScalaTest based tests; compilation now requires -DskipTests to compile
    the package without running tests (which might be necessary if you don't
    have a running Kinetica instance).

#### Fixed
-   Quoting table names for egress from Kinetica


### Version 6.2.2.3 -- 2019-04-10

#### Changed
-   Added quotes around schema/collection name, table name, and column names.
-   Allowing multiple periods in the table name (the first name will still
    be considered to be a schema/collection and table name separator).
-   Added a configuration parameter `table.name_contains_schema` with a default
    value of `true`, which indicates that a schema name should be extracted
    from the existing `table.name` parameter, if given any.


### Version 6.2.2.2 -- 2019-04-02

#### Changed
-   Changed the default timeout to 30 minutes
-   Changed the default retry count to 0

#### Fixed
-   Fixed egress issue related to importing string column type with the
    `datetime` subtype.
-   Fixed decimal ingestion failure (decimal in Kinetica can be mapped to any
    of float, double, int, long, string value based on the scale and precision).


### Version 6.2.2.1 -- 2019-02-13

#### Changed
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

