package com.kinetica.spark;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.ColumnProperty;
import com.gpudb.Record;
import com.gpudb.Type;
import com.gpudb.protocol.ClearTableRequest;
import com.gpudb.protocol.CreateTableRequest;
import com.gpudb.protocol.DeleteRecordsRequest;
import com.gpudb.protocol.GetRecordsByColumnRequest;
import com.gpudb.protocol.GetRecordsByColumnResponse;
import com.gpudb.protocol.ShowTableResponse;

import com.typesafe.scalalogging.LazyLogging;

import java.io.File;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType};
import org.scalatest.FunSuite;
import org.scalatest.BeforeAndAfterAll;
import org.scalatest.BeforeAndAfterEach;
import org.scalatest.Suite;

import scala.collection.JavaConversions._;
import scala.collection.JavaConverters._;
import scala.collection.{mutable, immutable};
import scala.util.Random;



/**
 * Convenience methods and shared objects need to go into this fixture
 * trait that the base class will mix-in.  When implementing tests
 * by inheriting from the base test class, all shared test cases (to
 * be run for both the v1 and v2 packages) need to go in a trait
 * that will extend this trait.  For example, look at
 * TestTableNameCollectionExtraction.scala. In order to actually run
 * those tests, we need to create two classes, one for the v1 package
 * and one for the v2 package.  Those classes will extend the base class
 * `SparkConnectorTestBase` and mix in the custom trait that has all the
 * test cases.
 */
trait SparkConnectorTestFixture
    extends BeforeAndAfterAll
            with LazyLogging { this: Suite =>

    // Names of the Kinetica Spark connector packages
    val m_default_package = "com.kinetica.spark";
    val m_v1_package      = "com.kinetica.spark.datasourcev1";
    val m_v2_package      = "com.kinetica.spark.datasourcev2";

    // Descriptions of the Kinetica Spark connector packages
    val m_default_package_descr = "[Default Package]";
    val m_v1_package_descr = "[Package DSv1]";
    val m_v2_package_descr = "[Package DSv2]";

    // Which package the tests use
    val m_package_descr   = m_default_package_descr;
    val m_package_to_test = m_default_package;


    var m_gpudb : GPUdb = null;

    // Various table related options
    val m_clearTableOptions = GPUdbBase.options( ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS,
                                                 ClearTableRequest.Options.TRUE );

    val m_createTableOptions = GPUdbBase.options( CreateTableRequest.Options.NO_ERROR_IF_EXISTS,
                                                  CreateTableRequest.Options.TRUE );

    val m_createTableInSchemaOptions = GPUdbBase.options( CreateTableRequest.Options.NO_ERROR_IF_EXISTS,
                                                              CreateTableRequest.Options.TRUE ).asScala;

    val m_createReplicatedTableInSchemaOptions = GPUdbBase.options( CreateTableRequest.Options.NO_ERROR_IF_EXISTS,
                                                                        CreateTableRequest.Options.TRUE,
                                                                        CreateTableRequest.Options.IS_REPLICATED,
                                                                        CreateTableRequest.Options.TRUE).asScala;

    val m_dropAllRecordsOptions = GPUdbBase.options( DeleteRecordsRequest.Options.DELETE_ALL_RECORDS,
                                                     DeleteRecordsRequest.Options.TRUE );

    // For temporary test data
    val m_temporary_directory_path: String = "_temp_test_data";
    val m_temporary_directory = new File( m_temporary_directory_path );

    // User given properties
    var m_host : String = "";
    var m_username : String = "";
    var m_password : String = "";
    var m_jdbc_url : String = "";
    var m_httpd_trust_store_path : String = "";
    var m_httpd_trust_store_pwd  : String = "";
    var m_jdbc_trust_store_path  : String = "";
    var m_jdbc_trust_store_pwd   : String = "";
    var m_bypass_ssl_cert_check     : Boolean = false;

    var m_sparkSession : SparkSession = _;


    // Names of tables and schemas that need to be deleted
    var m_tablesToClear : mutable.ListBuffer[String] = mutable.ListBuffer[String]();
    var m_schemasToClear : mutable.ListBuffer[String] = mutable.ListBuffer[String]();

    override def beforeAll() {

        // Note: In order to add command-line parameters for the test,
        //       the following needs to be done:
        //   1) Add the parameter to the org.scalatest plugin configuration
        //      entry `argLine` in the POM.  Just add it to the end, with
        //      the format ' -Daaa=${xxx}'.
        //      The whitespace is important.  Also, note that the
        //      'aaa' is what you need to use for the first parameter
        //      to System.getProperty() below.  On the other hand, `xxx`
        //      is what the user would pass in from the command line,
        //      as follows:
        //      > mvn test -Dxxx=some_value
        //      Also note that 'xxx' should start with a 'k' so that it
        //      does not get confused with an environment variable (which
        //      happened with 'url' and 'username', hence 'kurl' and
        //      'kusername').
        //   2) Document the parameter in the README.md file for testing.
        //   3) Back in the POM, add an empty string as the default value
        //      for the user-facing parameter 'xxx' in the <properties>
        //      <!-- Parameters for ScalaTest> section.  Otherwise, the code
        //      gets something like "${xxx}", and System.getProperty()'s
        //      default value (the 2nd parameter) is NEVER applied.
        //      Example: <kurl>""</kurl>

        // Parse any user given parameters
        m_host = System.getProperty("url", "http://127.0.0.1:9191");
        // Need to check for an empty string explicitly since the behavior
        // of the scalatest-maven-plugin doesn't actually allow the default
        // value specified above to be used (it always thinks the argument
        // is passed in, even if the user doesn't specify something at the
        // command-line).
        if ( m_host.isEmpty ) {
            m_host = "http://127.0.0.1:9191";
        }
        logger.info( s"User given parameter host URL value: ${m_host}" );

        // Parse user given username and password, if any
        m_username = System.getProperty("username", "");
        m_password = System.getProperty("password", "");
        logger.info( s"User given parameter username value: ${m_username}" );

        // SSL certificate trust store related
        m_httpd_trust_store_path = System.getProperty("httpdTrustStorePath", "");
        m_httpd_trust_store_pwd  = System.getProperty("httpdTrustStorePassword", "");
        m_jdbc_trust_store_path  = System.getProperty("jdbcTrustStorePath", "");
        m_jdbc_trust_store_pwd   = System.getProperty("jdbcTrustStorePassword", "");

        logger.info( s"User given flag for HTTPD cert verification trust store path:     '${m_httpd_trust_store_path}'" );
        logger.info( s"User given flag for HTTPD cert verification trust store password: '${m_httpd_trust_store_pwd}'" );
        logger.info( s"User given flag for ODBC cert verification trust store path:      '${m_jdbc_trust_store_path}'" );
        logger.info( s"User given flag for ODBC cert verification trust store password:  '${m_jdbc_trust_store_pwd}'" );

        // The Java API needs to know if the SSL certificate verification
        // needs to be bypassed (will do only if no HTTPD trust store is given.
        m_bypass_ssl_cert_check = (m_httpd_trust_store_path.isEmpty || m_httpd_trust_store_pwd.isEmpty);
        logger.info( s"Passing value for SSL cert verification *bypassing* flag to the Java API: ${m_bypass_ssl_cert_check}" );

        if ( !m_bypass_ssl_cert_check ) {
            logger.info( s"Setting system properties with the trust store path and password" );
            System.setProperty("javax.net.ssl.trustStore", m_httpd_trust_store_path);
            System.setProperty("javax.net.ssl.trustStorePassword", m_httpd_trust_store_pwd);
        }

        val db_options = new GPUdbBase.Options().setBypassSslCertCheck( m_bypass_ssl_cert_check );
        if ( !m_username.isEmpty ) {
            db_options.setUsername( m_username ).setPassword( m_password );
        }

        // Create a DB with the given options (username and password, if any)
        m_gpudb = new GPUdb( m_host, db_options );

        // Get the JDBC URL
        m_jdbc_url = get_jdbc_url();

        // Create a directory for any test files that may need to be generated
        m_temporary_directory.mkdir();
    }   // end beforeAll


    override def afterAll() {
        // Delete all files created in the temporary test data diretcotry
        remove_directory( m_temporary_directory );
    }


    // Given the GPUdb connection, get the JDBC URL
    def get_jdbc_url(): String = {
        // Get the API version
        val version = GPUdbBase.getApiVersion();
        logger.info(s"Kinetica Version: ${version}");
        val host = m_gpudb.getURL().getHost();
        var jdbc_url : String = "";
        if (s"${version(0)}".toInt > 6) {
            jdbc_url = s"jdbc:kinetica://${host}:9191";
        } else {
            jdbc_url = s"jdbc:simba://${host}:9292";
        }
        logger.info(s"JDBC URL: ${jdbc_url}");
        return jdbc_url;
    }


    /**
     * Create a mutable map with some default spark connector options.
     * Does set values for parameters that have default values (with the
     * default values)--just so that its'
     * Sets connection related parameters that would be common to all
     * tests.
     * Return the map.
     */
    def get_default_spark_connector_options(): mutable.Map[String, String] = {
        var options: mutable.Map[String, String] = null;
        options = mutable.Map[String, String]( "database.url" -> m_host,
                                       "database.jdbc_url" -> m_jdbc_url,
                                       "database.username" -> m_username,
                                       "database.password" -> m_password,
                                       "database.retry_count" -> "0",
                                       "database.timeout_ms" -> "1800000",
                                       "ingester.analyze_data_only" -> "false",
                                       "ingester.batch_size" -> "10000",
                                       "ingester.flatten_source_schema" -> "false",
                                       "ingester.ip_regex" -> "",
                                       // "ingester.multi_head" -> "true",
                                       "ingester.multi_head" -> "false",
                                       "ingester.num_threads" -> "4",
                                       "ingester.use_snappy" -> "false",
                                       "spark.num_partitions" -> "4",
                                       "table.create" -> "false",
                                       "table.is_replicated" -> "false",
                                       "table.truncate" -> "false",
                                       "table.truncate_to_size" -> "false",
                                       "table.update_on_existing_pk" -> "false",
                                       "table.use_templates" -> "false",
                                       "table.append_new_columns" -> "false",
                                       "table.map_columns_by_name" -> "true"
                                );
        // Set SSL related flags only if provided by the user
        if ( !m_httpd_trust_store_path.isEmpty ) {
            options( "ssl.truststore_jks" ) = m_httpd_trust_store_path;
        }
        if ( !m_httpd_trust_store_pwd.isEmpty ) {
            options( "ssl.truststore_password" ) = m_httpd_trust_store_pwd;
        }
        if ( !m_jdbc_trust_store_path.isEmpty ) {
            options( "ssl.odbc_truststore_jks" ) = m_jdbc_trust_store_path;
        }
        if ( !m_jdbc_trust_store_pwd.isEmpty ) {
            options( "ssl.odbc_truststore_password" ) = m_jdbc_trust_store_pwd;
        }
        return options;
    }

    // --------------- Methods for Test Data Related Files -----------------

    /**
     * Delete all the files and directories in the given path.
     */
    def remove_directory( directory: File ) : Unit = {
        if ( directory.isDirectory()  ) {
            // Clean up all files and directories insides
            val files = directory.listFiles();
            if ( (files != null) && (files.length > 0) ) {
                for ( file <- files ) {
                    remove_directory( file );
                }
            }
            // Delete the directory
            directory.delete();
        } else {
            directory.delete();
        }
    }


    /**
     * Create a directory path with the given directory name in the testing framework's
     * temporary data directory.  Does not create the new directory.
     */
    def get_temp_directory_path( dirPath: String ) : String = {
        val directory = new File( m_temporary_directory, dirPath );
        return directory.getAbsolutePath();
    }

    // ------- Methods for Testing Kinetica Tables etc. via the Java API -------

    /**
     * Add to the list of tables to clear at the end of the test
     */
    def mark_table_for_deletion_at_test_end( tableName: String ) : Unit = {
        m_tablesToClear += tableName;
        return;
    }

    /**
     * Add to the list of schemas to clear at the end of the test
     */
    def mark_schema_for_deletion_at_test_end( name: String ) : Unit = {
        m_schemasToClear += name;
        return;
    }

    /**
     * Delete the given table.
     */
    def clear_table( tableName: String ) : Unit = {
        val request = new ClearTableRequest();
        request.setTableName( tableName );
        request.setOptions( m_clearTableOptions );
        logger.debug( s"Attempting to clear table '$tableName'" );
        m_gpudb.clearTable( request );
        logger.debug( s"Cleared table '$tableName'" );
        return;
    }


    /**
     * Delete the given schema.
     */
    def clear_schema( schemaName: String ) : Unit = {
        try {
            m_gpudb.showSchema( schemaName, null );
            logger.debug( s"Attempting to drop schema '$schemaName'" );
            m_gpudb.dropSchema( schemaName, null );
            logger.debug( s"Dropped schema '$schemaName'" );
            return;
        } catch {
            case e: Throwable => return;
        }
    }

    /**
     * Delete all the records in the given table.
     */
    def drop_all_records( tableName: String ) : Unit = {
        val request = new DeleteRecordsRequest();
        request.setTableName( tableName );
        request.setOptions( m_dropAllRecordsOptions );
        m_gpudb.deleteRecords( request );
        return;
    }


    /**
     * Given a table name, get the current table size.
     */
    def get_table_size( table_name: String ) : scala.Long = {
        val show_table = m_gpudb.showTable( s"${table_name}",
                                            immutable.Map[String, String]("get_sizes" -> "true").asJava );
        val table_props = show_table.getProperties().asScala;
        val table_size = show_table.getTotalSize();
        return table_size;
    }


    /**
     * Given a table name, check if the table exists (as is, without any special
     * name handling.)
     */
    def does_table_exist( table_name: String ) : Boolean = {
        return m_gpudb.hasTable( table_name, null ).getTableExists();
    }

    /**
     * Given a schema name, check if the schema exists.
     */
    def does_schema_exist( schema_name: String ) : Boolean = {
        try {
            val info = m_gpudb.showSchema( schema_name, null );
            return true;
        } catch {
            case e: Throwable => return false;
        }
    }

    /**
     * Create a schema by name if it does not exist.
     */
    def create_schema( schema_name: String ) : String = {
        try {
            val info = m_gpudb.showSchema( schema_name, null );
        } catch {
            case e: Throwable => {
                m_gpudb.createSchema( schema_name, null )
            }
        }
        return schema_name;
    }

    /**
     * Given a table name, retrieve its schema name.
     * Return None if the table does not exist.
     */
    def get_table_schema_name( table_name: String ) : Option[String] = {
        if ( does_table_exist( table_name ) ) {
            val show_table = m_gpudb.showTable( s"${table_name}", null );
            val schema_name = show_table.getAdditionalInfo().get( 0 )
                                                            .get("schema_name");
            return Option.apply( schema_name );
        }

        // Table does not exist, return an empty string
        return None;
    }


    /**
     * Fetch the given columns of the given records (based on indices)
     * from the given table.  Optionally give a sort column and order.
     */
    def get_records_by_column( table_name: String, column_names: List[String],
                               offset: Int, numRecords: Int,
                               sortByColumn: Option[String],
                               sortOrder: Option[String] )
        : java.util.List[Record] = {

        val options: java.util.Map[String, String] = new java.util.HashMap[String, String]();
        if ( !sortByColumn.isEmpty ) {
            options.put( GetRecordsByColumnRequest.Options.SORT_BY,
                         sortByColumn.get );
            options.put( GetRecordsByColumnRequest.Options.SORT_ORDER, sortOrder.get );
        }

        // Set up the request
        val getRecordsByColumnReq = new GetRecordsByColumnRequest( table_name, column_names,
                                                                   offset, numRecords,
                                                                   options );
        // Make the request
        val getRecordsByColumnResponse = m_gpudb.getRecordsByColumn( getRecordsByColumnReq );
        return getRecordsByColumnResponse.getData();
    }


    /**
     * Delete all existing records of the given table
     */
    def delete_all_records( table_name: String ) : Unit = {
        m_gpudb.deleteRecords( s"${table_name}",
                               null,
                               immutable.Map[String, String]("delete_all_records" -> "true").asJava );
        return;
    }


    /**
     * Given a table name, names of columns to compare, the sorting column name,
     * and the expected data, fetch the appropriate data from the table in a sorted
     * manner and compare to the expected data.  The data comparison happens with assertions;
     * so no value is returned.
     */
    def compare_table_data( table_name: String,
                            columns_to_compare: List[String],
                            sort_column_name: String,
                            expected_data: Seq[scala.collection.immutable.Map[String,Any]] )
        : Unit = {
        // Fetch the data from the table first
        val fetched_records = get_records_by_column( table_name,
                                                     columns_to_compare,
                                                     0, GPUdbBase.END_OF_SET.toInt,
                                                     Option.apply( sort_column_name ),
                                                     Option.apply( "ascending" ) );
        // Compare the fetched data per row
        for ( i <- 0 until expected_data.length ) {
            // Compare each column for the current row
            for ( column_name <- columns_to_compare ) {
                val expected = expected_data(i)(           column_name );
                val actual   = fetched_records.get(i).get( column_name );
                assert( (expected == actual),
                        s"Fetched value of record #$i '$actual' should be '$expected' for '$column_name'" );
            }
        }
    }



    // --------- Data Generator Functions for Testing Convenience ---------

    /**
     * Generate a random string of alphanumeric characters of the given length
     */
    def getRandomKineticaUsername( length: Integer ) : String = {
        val chars = ('a' to 'z') ++ ('0' to '9');
        val sb = new StringBuilder;
        for ( i <- 1 to length ) {
            val j = Random.nextInt( chars.length );
            sb.append( chars( j ) );
        }
        return sb.toString();
    }



    // --------- DataFrame Generator Functions for Testing Convenience ---------

    /**
     * Create a dataframe with two nullable integer columns 'x' and 'y' and
     * generate the given number of random rows.
     */
    def createDataFrame( data: IndexedSeq[Seq[Any]],
    // def createDataFrame( data: scala.collection.immutable.IndexedSeq[Seq[Any]],
                         schema: StructType ) : DataFrame = {
        // Generate the RDD from the data
        val rddRows = m_sparkSession.sparkContext.parallelize( data.map( x => Row(x:_*) ) );
        // Generate the dataframe from the RDD
        val df = m_sparkSession.sqlContext.createDataFrame( rddRows, schema );

        return df;
    }


    /**
     * Create a dataframe with two nullable integer columns 'x' and 'y' and
     * generate the given number of random rows.
     */
    def createDataFrame( data: Seq[Row],
                         schema: StructType ) : DataFrame = {
        // Generate the RDD from the data
        val rddRows = m_sparkSession.sparkContext.parallelize( data );
        // Generate the dataframe from the RDD
        val df = m_sparkSession.sqlContext.createDataFrame( rddRows, schema );

        return df;
    }


    /**
     * Create a dataframe with two nullable integer columns 'x' and 'y' and
     * generate the given number of random rows.
     */
    def createDataFrameTwoIntNullableColumns( numRows: Int ) : DataFrame = {
        logger.debug("Creating a dataframe with random data in it");
        // Generate random data
        val numColumns = 2;
        val data = (1 to numRows).map(_ => Seq.fill( numColumns )(Random.nextInt) );
        // Generate the schema
        val schema = StructType( StructField( "x", IntegerType, true ) ::
                                 StructField( "y", IntegerType, true ) :: Nil );
        // Generate the RDD from the data
        val rdd = m_sparkSession.sparkContext.parallelize( data.map( x => Row(x:_*) ) );
        // Generate the dataframe from the RDD
        val df = m_sparkSession.sqlContext.createDataFrame( rdd, schema );

        // Double check the dataframe has the desired number of rows
        assert(df.count() == numRows);

        return df;
    }



    // -------- Kinetica Table Generator Functions for Testing Convenience ---------


    /**
     * Create a Kinetica table with the given name with the given type.
     * Optionally generate the given number of random records.
     * If table name is a fully qualified name with schema, ensure that Kinetica puts
     * it in that schema.  Clear any pre-existing table with the same
     * name first.
     */
    def createKineticaTableWithGivenColumns( tableName: String,
                                             columns: mutable.ListBuffer[Type.Column],
                                             numRows: Int ) : String = {
        // Create a type object from the columns
        val type_ = new Type( columns.toList.asJava );

        // Delete any pre-existing table with the same name
        clear_table( tableName );

        if ( tableName contains "." ) {
            var schemaName = tableName.split("\\.")( 0 );
            try {
                m_gpudb.showSchema(schemaName, null);
            } catch {
                case e: Throwable => {
                    // If the given schema does not exist, create schema
                    m_gpudb.createSchema(schemaName, null);
                    logger.debug( s"Created schema $schemaName via the Java API" );
                    mark_schema_for_deletion_at_test_end( schemaName );
                }
            }
        }

        // Create the table
        var resp = m_gpudb.createTable( tableName, type_.create( m_gpudb ),
                             m_createTableInSchemaOptions );
        var createdTableName = resp.getInfo().get(com.gpudb.protocol.CreateTableResponse.Info.QUALIFIED_TABLE_NAME)
        
        logger.debug( s"Created table $createdTableName via the Java API" );

        // Generate some random data, if desired by the caller
        if ( numRows > 0) {
            m_gpudb.insertRecordsRandom( createdTableName, numRows, null );
        }

        mark_table_for_deletion_at_test_end( createdTableName );
        return createdTableName;
    }



    /**
     * Create a REPLICATED Kinetica table with the given name with the given type.
     * Optionally generate the given number of random records.  If a
     * collection name is given, ensure that Kinetica puts
     * it in that collection.  Clear any pre-existing table with the same
     * name first.
     */
    def createReplicatedKineticaTableWithGivenColumns( tableName: String,
                                                       columns: mutable.ListBuffer[Type.Column],
                                                       numRows: Int ) : String = {
        // Create a type object from the columns
        val type_ = new Type( columns.toList.asJava );

        // Delete any pre-existing table with the same name
        clear_table( tableName );

        if ( tableName contains "." ) {
            var schemaName = tableName.split("\\.")( 0 );
            try {
                m_gpudb.showSchema(schemaName, null);
            } catch {
                case e: Throwable => {
                    // If the given schema does not exist, create schema
                    m_gpudb.createSchema(schemaName, null);
                    logger.debug( s"Created schema $schemaName via the Java API" );
                    mark_schema_for_deletion_at_test_end( schemaName );
                }
            }
        }

        // Create the replicated table
        var resp = m_gpudb.createTable( tableName, type_.create( m_gpudb ),
                             m_createReplicatedTableInSchemaOptions );
        var createdTableName = resp.getInfo().get(com.gpudb.protocol.CreateTableResponse.Info.QUALIFIED_TABLE_NAME)
        
        logger.debug( s"Created table $createdTableName via the Java API" );

        // Generate some random data, if desired by the caller
        if ( numRows > 0) {
            m_gpudb.insertRecordsRandom( createdTableName, numRows, null );
        }

        mark_table_for_deletion_at_test_end( createdTableName );
        return createdTableName;
    }

        /**
     * Helper function converting Date, Time and DateTime strings to compatible
     * Kinetica format and be local to the specified timezone
     */
    def getExpectedLongTimeStampValue ( value: String, timeZone : String ) : Long = {
        val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy[-][/][.]MM[-][/][.]dd[[ ]['T']HH:mm[:ss][.SSS][ ][XXX][Z][z][VV][x]]");
        val zoneID = java.util.TimeZone.getTimeZone( timeZone ).toZoneId();

        val MAX_DATE = 29379542399999L;
        val MIN_DATE = -30610224000000L;


        // Get the number of milliseconds since the epoch
        var sinceepoch : Long = 0;

        try {
            // Try parsing a full timestamp with date, time and timezone
            val parsed = java.time.ZonedDateTime.parse( value.toString(), formatter );
            // get timezone offset in seconds
            val offset = parsed.get(java.time.temporal.ChronoField.OFFSET_SECONDS);
            // modify the date with extracted offset, not user-defined timezone,
            // because offset is already calculated against UTC
            sinceepoch = parsed
                .toLocalDateTime()
                .atZone(  ZoneId.of("UTC")  )
                .toInstant()
                .toEpochMilli();
            if( sinceepoch > MAX_DATE ) {
                sinceepoch = MAX_DATE;
            } else if( sinceepoch < MIN_DATE ) {
                sinceepoch = MIN_DATE;
            }
        } catch {
            case e: java.time.format.DateTimeParseException => {
                try {
                    // Try parsing a full local timestamp with date and time
                    val offset = java.time.ZoneId.systemDefault().getRules()
                                                 .getOffset(java.time.Instant.now())
                                                 .getTotalSeconds();
                    sinceepoch = java.time.LocalDateTime.parse( value.toString(), formatter )
                        .atZone(  ZoneId.of("UTC")  )
                        .toInstant()
                        .toEpochMilli();
                    if( sinceepoch > MAX_DATE ) {
                        sinceepoch = MAX_DATE;
                    } else if( sinceepoch < MIN_DATE ) {
                        sinceepoch = MIN_DATE;
                    }
                } catch {
                    case e: java.time.format.DateTimeParseException => {
                        // Try parsing just the date
                        sinceepoch = java.time.LocalDate.parse( value.toString(), formatter )
                            .atStartOfDay()
                            .toInstant( java.time.ZoneOffset.UTC)
                            .toEpochMilli();
                    }
                }
            }
        }
        return sinceepoch;
    }



}   // end trait SparkConnectorTestFixture





/**
 * The base class for all Kinetica Spark connector tests.  Read comments above
 * trait `SparkConnectorTestFixture` to see how to write tests.
 *
 */
class SparkConnectorTestBase
    extends FunSuite
            with BeforeAndAfterEach
            with SparkConnectorTestFixture
            with LazyLogging {

    // Create the spark session before each test
    override def beforeEach() {
        // Obtain the spark session
        m_sparkSession = SparkSession.builder().appName("Kinetica Spark Connector Tests")
            .master("local")
            .config("", "")
            .getOrCreate();
    }

    // Tear down the spark session after each test
    override def afterEach() {
        // Clear the tables
        m_tablesToClear.toList.foreach( tableName => clear_table( tableName ) );
        // Empty the list content
        m_tablesToClear.clear();

        m_schemasToClear.toList.foreach( schemaName => clear_schema( schemaName ) );
        // Empty the list content
        m_schemasToClear.clear();

        // Stop the spark session
        m_sparkSession.stop();
    }


}  // end SparkConnectorTestBase

