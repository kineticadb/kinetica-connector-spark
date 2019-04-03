# Kinetica Spark Connector Changelog

## Version 7.0

### Version 7.0.0.2 -- 2019-04-01

#### Changes
-   Changed the default timeout to 30 minutes
-   Changed the default retry count to 0

#### Bugfix
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

