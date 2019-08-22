package com.kinetica.spark;

import com.gpudb.ColumnProperty;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.Record;
import com.gpudb.Type;
import com.gpudb.protocol.GrantPermissionTableRequest;
import com.gpudb.protocol.ShowSystemPropertiesRequest;


import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.{FloatType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
        StructField,
        StructType};
import com.typesafe.scalalogging.LazyLogging;
import scala.collection.JavaConverters._;
import scala.collection.{mutable, immutable};
import scala.collection.mutable.ListBuffer;
import scala.util.Random;

import org.scalatest.FunSuite;



/**
 * This trait contains test cases for bug fixes for the spark connector.
 *
 * In order to run these tests, a class is needed that mixes in this trait
 * and uses the `testsFor()` method which invokes the given behavior function.
 *
 */
trait SparkConnectorBugFixes
    extends SparkConnectorTestFixture { this: FunSuite =>

    /**
     * Tests for various bug fixes.
     */
    def bugFixes( package_to_test: String, package_description: String ) {

        /**
         * Test for ingesting string timestamps into long timestamps.
         */
        test(s"""$package_description KECO-1396: String timestamp should be able
             | to be ingested in a long timestamp column without
             | issues""".stripMargin.replaceAll("\n", "") ) {

            // Create a table type with one long, timestamp column
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            val sort_col_name = "i";
            val ts_col_name   = "timestamp";
            columns += new Type.Column( sort_col_name, classOf[java.lang.Integer] );
            columns += new Type.Column( ts_col_name, classOf[java.lang.Long],
                                        ColumnProperty.NULLABLE,
                                        ColumnProperty.TIMESTAMP );
    
            // Create the table (but clear any pre-existing ones)
            val tableName = s"keco_1396_long_timestamp_${package_description}";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Set the default timezone appropriately
            val timeZone = "GMT+0600";

            // The expected values would be of Kinetica format and be local to the specified timezone
            def getExpectedLongTimeStampValue ( value: String, timeZone : String ) : Long = {
                val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy[-][/][.]MM[-][/][.]dd[[ ]['T']HH:mm[:ss][.SSS][ ][XXX][Z][z][VV][x]]");
                val zoneID = TimeZone.getTimeZone( "GMT+0600" ).toZoneId();
                return java.time.LocalDateTime.parse( value, formatter )
                              .atZone( zoneID ).toInstant().toEpochMilli();                
            }

            // Create some test data
            val timestamp_data = "1950-02-20T05:40:18.386-05:00" ::
                                 "1950/02/20T05:40:18.386-05:00" ::
                                 "1950.02.20T05:40:18.386-05:00" ::
                                 "1950-02-20 05:40:18.386-05:00" ::
                                 "1950-02-20T05:40:18.386" ::
                                 "1950-02-20 05:40" :: Nil;
            var data = Seq.empty[ Row ];
            var expected_values = Seq.empty[ Map[String, Long] ];
            for ( i <- 0 until timestamp_data.length ) {
                val value = timestamp_data(i);
                data = data :+ Row( i, value );
                expected_values = expected_values :+ Map( ts_col_name -> getExpectedLongTimeStampValue( value,
                                                                                                        timeZone ) );
            }

            // Generate the appropriate schema and insert the data
            val schema = StructType( StructField( sort_col_name, IntegerType, true ) ::
                                     StructField( ts_col_name,   StringType, true ) :: Nil );
            val df = createDataFrame( data, schema );
            logger.debug(s"Created a dataframe with random timestamp string in it (${df.count} rows)");

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"          ) = "false";
            options( "table.name"            ) = tableName;
            options( "ingester.use_timezone" ) = timeZone;

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            val table_size = get_table_size( tableName );
            assert( (table_size == data.length), s"Table size ($table_size) should be ${data.length}" );

            // Check correctness of the data
            val columns_to_compare = ts_col_name :: Nil;
            compare_table_data( tableName, columns_to_compare, sort_col_name, expected_values );
        }  // end test #1 for KECO-1396


        /**
         * Test for ingesting string timestamps into string timestamps (with
         * non-Kinetica formats).
         */
        test(s"""$package_description KECO-1396: Timestamp date, time, datetime
             | with various formats (including non-Kinetica formats) should be
             | able to be ingested without
             | issues""".stripMargin.replaceAll("\n", "") ) {

            // This test case has control data; correctness of data
            // is checked after ingestion
            
            // Create a table type with date, time, datetime columns
            val sort_col_name = "i";
            val col1_name     = "date";
            val col2_name     = "time";
            val col3_name     = "datetime";
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( sort_col_name, classOf[java.lang.Integer] );
            columns += new Type.Column( col1_name, classOf[java.lang.String],
                                        ColumnProperty.NULLABLE,
                                        ColumnProperty.DATE );
            columns += new Type.Column( col2_name, classOf[java.lang.String],
                                        ColumnProperty.NULLABLE,
                                        ColumnProperty.TIME );
            columns += new Type.Column( col3_name, classOf[java.lang.String],
                                        ColumnProperty.NULLABLE,
                                        ColumnProperty.DATETIME );

            // Create the table (but clear any pre-existing ones)
            val tableName = s"keco_1396_string_date_time_datetime_${package_description}";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Create some test data
            val dateFormat = new SimpleDateFormat( "yyyy/MM/dd" );
            // Set the default timezone appropriately
            val timeZone = "GMT+0600";
            TimeZone.setDefault( TimeZone.getTimeZone( timeZone ) );
            // The test data don't conform to the Kinetica format, for the most part
            val data = Seq(
                           Row( 0,
                                Timestamp.from( dateFormat.parse( "1970/02/01" ).toInstant() ),
                                "09:50:00.005-04:00",
                                Timestamp.from( ZonedDateTime
                                                .from( DateTimeFormatter
                                                       .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                                                       .parse( "1950-02-20T05:40:18.386-05:00" ) )
                                                .toInstant() ) ),
                           Row( 1,
                                Timestamp.from( ZonedDateTime
                                                .from( DateTimeFormatter
                                                       .ofPattern("yyyy.MM.dd'T'HH:mm:ss.SSSXXX")
                                                       .parse( "1970.01.02T12:23:34.123Z" ) )
                                                .toInstant() ),
                                "09:50:00.004+08:00",
                                Timestamp.from( ZonedDateTime
                                                .from( DateTimeFormatter
                                                       .ofPattern("MM-dd-yyyy'T'HH:mm:ss.SVV")
                                                       .parse( "02-20-1908T12:23:34.2Z" ) )
                                                .toInstant() ) ),
                           Row( 2,
                                Timestamp.from( ZonedDateTime
                                                .from( DateTimeFormatter
                                                       .ofPattern("dd-MM-yyyy'T'HH:mm:ss.SSSXXX")
                                                       .parse( "13-01-1970T12:23:34.054+07:00" ) )
                                                .toInstant() ),
                                "09:50:00.123456",
                                Timestamp.from( ZonedDateTime
                                                .from( DateTimeFormatter
                                                       .ofPattern("dd-MM-yyyy'T'HH:mm:ss.SSSXXX")
                                                       .parse( "04-01-1970T00:00:00.000+06:00" ) )
                                                .toInstant() ) )
                           );
            // The expected values would be of Kinetica format and be local to the specified timezone
            val expected_values = Seq( Map("date" -> "1970-02-01",
                                           "time" -> "19:50:00.005",
                                           "datetime" -> "1950-02-20 16:40:18.386" ),
                                       Map("date" -> "1970-01-02",
                                           "time" ->"07:50:00.004",
                                           "datetime" -> "1908-02-20 18:23:34.200" ),
                                       Map("date" -> "1970-01-13",
                                           "time" -> "09:50:00.123",
                                           "datetime" -> "1970-01-04 00:00:00.000" )
                );

            // Generate the appropriate schema
            val schema = StructType( StructField( sort_col_name, IntegerType, true ) ::
                                     StructField( col1_name,     TimestampType, true ) ::
                                     StructField( col2_name,     StringType, true ) ::
                                     StructField( col3_name,     TimestampType, true ) :: Nil );
            val df = createDataFrame( data, schema );
            logger.debug(s"Created a dataframe with various time & date values in it with (${df.count} rows)");

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"          ) = "false";
            options( "table.name"            ) = tableName;
            options( "ingester.use_timezone" ) = timeZone;

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            val table_size = get_table_size( tableName );
            assert( (table_size == df.count), s"Table size ($table_size) should be ${df.count}" );

            // Check the data
            val columns_to_compare = col1_name :: col2_name :: col3_name :: Nil;
            compare_table_data( tableName, columns_to_compare, sort_col_name, expected_values );
        }  // end test #2 for KECO-1396


        /**
         * Test for ingesting string and long timestamps after egressing via the connector
         */
        test(s"""$package_description KECO-1396: Timestamp, date, time, datetime
             | should be able to be ingested without problems after obtaining the
             | data via connector egress feature""".stripMargin.replaceAll("\n", "") ) {

            // This test case does not have any control data; it just egresses
            // some (random) data, and tries to ingest it back, without checking
            // for any correctness
            
            // Create a table type with date, time, datetime columns
            val sort_col_name = "i";
            val col1_name     = "date";
            val col2_name     = "time";
            val col3_name     = "datetime";
            val col4_name     = "timestamp";
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( sort_col_name, classOf[java.lang.Integer] );
            columns += new Type.Column( col1_name, classOf[java.lang.String],
                                        ColumnProperty.NULLABLE,
                                        ColumnProperty.DATE );
            columns += new Type.Column( col2_name, classOf[java.lang.String],
                                        ColumnProperty.NULLABLE,
                                        ColumnProperty.TIME );
            columns += new Type.Column( col3_name, classOf[java.lang.String],
                                        ColumnProperty.NULLABLE,
                                        ColumnProperty.DATETIME );
            columns += new Type.Column( col4_name, classOf[java.lang.Long],
                                        ColumnProperty.NULLABLE,
                                        ColumnProperty.TIMESTAMP );

            // Create the table (but clear any pre-existing ones)
            val tableName  = s"keco_1396_date_time_datetime_timestamp_${package_description}";
            val numRecords = 10;
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, numRecords );

            // // Need to mark the table name for post-test clean-up
            // mark_table_for_deletion_at_test_end( tableName );

            // Get the appropriate ingest options
            var ingest_options = get_default_spark_connector_options();
            ingest_options( "table.create" ) = "false";
            ingest_options( "table.name"   ) = tableName;
            // ingest_options( "ingester.use_timezone" ) = timeZone;

            // Get the appropriate egress options
            var egress_options = get_default_spark_connector_options();
            egress_options( "table.name" ) = tableName;

            // Test the Java API path
            // ----------------------

            // Get the data out using the connecotr via the Java API (when no filtering is necessary)
            val fetched_records_native = m_sparkSession.sqlContext.read.format( package_to_test )
                                              .options( egress_options ).load();
            logger.debug( s"Extracted the data from table ${tableName} via the connector (no filter --> use the Java API)" );

            // Re-insert the data
            try {
                // Write the data to a CSV file and read back from it; note that
                // it's important to get a path in the temporary data file path
                logger.debug( s"Saving data from ${tableName} into a CSV file" );
                val csv_location = get_temp_directory_path( s"keco_1396_without_filters_${package_to_test}" );
                fetched_records_native.write.format("csv")
                    .mode("overwrite")
                    .option("header", "true")
                    .save( csv_location );
                logger.debug( s"Reading back the data from the CSV file" );
                val df_csv = m_sparkSession.sqlContext.read.format("csv")
                    .option("header", "true")
                    .option("inferSchema", "false")
                    .option("delimiter", ",")
                    .csv( csv_location );

                // Write the data read from the CSV file back into Kinetica using the connector
                logger.debug( s"Writing the data to ${tableName}" );
                df_csv.write.format( package_to_test ).options( ingest_options ).save();
                
                // Check that the table size is correct
                val table_size = get_table_size( tableName );
                // Generated 10 random records, inserted all of them again once
                val expected_table_size = (numRecords * 2);
                assert( (table_size == expected_table_size), s"Table size should be $expected_table_size; got $table_size" );
            } catch {
                case e: com.kinetica.spark.util.table.KineticaException => {
                    logger.debug( s"Got KINETICA exception {}", e );
                    assert( (false), s"Re-ingesting data obtained by egress via the connector from Kinetica should not fail (without filters)" );
                }
                case e2: java.lang.RuntimeException => {
                    logger.debug( s"Got RUNTIME exception {}", e2 );
                    assert( (false), s"Re-ingesting data obtained by egress via the connector from Kinetica should not fail (without filters)" );
                }
            }
            
            
            // Test the JDBC connector path
            // ----------------------------
            // Get the data out using the connecotr via the JDBC connector (when filtering IS necessary)
            val filter_expression = s"i < 1000";
            val fetched_records_jdbc = m_sparkSession.sqlContext.read
                                          .format( package_to_test )
                                          .options( egress_options ).load()
                                          .filter( filter_expression );
            logger.debug( s"Extracted the data from table ${tableName} via the connector (no filter --> use the Java API)" );

            // Re-insert the data
            try {
                // Write the data to a CSV file and read back from it; note that
                // it's important to get a path in the temporary data file path
                logger.debug( s"Saving data from ${tableName} into a CSV file" );
                val csv_location = get_temp_directory_path( s"keco_1396_with_filters_${package_to_test}" );
                fetched_records_native.write.format("csv")
                    .mode("overwrite")
                    .option("header", "true")
                    .save( csv_location );
                logger.debug( s"Reading back the data from the CSV file" );
                val df_csv = m_sparkSession.sqlContext.read.format("csv")
                    .option("header", "true")
                    .option("inferSchema", "false")
                    .option("delimiter", ",")
                    .csv( csv_location );

                // Write the data read from the CSV file back into Kinetica using the connector
                logger.debug( s"Writing the data to ${tableName}" );
                df_csv.write.format( package_to_test ).options( ingest_options ).save();
                
                // Check that the table size is correct
                val table_size = get_table_size( tableName );
                // Generated 10 random records, inserted all of them once; got
                // these 20 out, and now have ingested them again; so quadurple
                // the original size
                val expected_table_size = (numRecords * 4);
                assert( (table_size == expected_table_size), s"Table size should be $expected_table_size; got $table_size" );
            } catch {
                case e: com.kinetica.spark.util.table.KineticaException => {
                    logger.debug( s"Got KINETICA exception {}", e );
                    assert( (false), s"Re-ingesting data obtained by egree via the connector from Kinetica should not fail (with filters)" );
                }
                case e2: java.lang.RuntimeException => {
                    logger.debug( s"Got RUNTIME exception {}", e2 );
                    assert( (false), s"Re-ingesting data obtained by egree via the connector from Kinetica should not fail (with filters)" );
                }
            }
        }  // end test #3 for KECO-1396


        
        /**
         * Test for ingesting numbers into floats.
         */
        test(s"""$package_description KECO-1355: Numeric values should be able to
             | ingest into float column
             | without issues""".stripMargin.replaceAll("\n", "") ) {

            // We need a table with a float column
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( "float_col", classOf[java.lang.Float] );

            // Create the table
            val tableName = s"keco_1355_${package_description}";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "false";
            options( "table.name"                 ) = tableName;

            // Test inserting double into float
            // --------------------------------
            logger.debug(s"Test inserting double into float");

            // Create some double data
            val numRows = 1;
            val numColumns = 1;
            val data_d = (1 to numRows).map(_ => Seq.fill( numColumns )( Random.nextDouble() ) );

            // Generate the schema with a double type
            val schema_d = StructType( StructField( "float_col", DoubleType, true ) :: Nil );
            
            val df1 = createDataFrame( data_d, schema_d );
            logger.debug(s"Created a dataframe with double values in it with (${df1.count} rows)");

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df1.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            var table_size = get_table_size( tableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );

            // Delete all records from the table for the next test
            delete_all_records( tableName );
            
            // Test inserting float into float
            // --------------------------------
            logger.debug(s"Test inserting float into float");

            // Generate the schema and data with a float type
            val schema_f = StructType( StructField( "float_col", FloatType, true ) :: Nil );
            val data_f  = (1 to numRows).map(_ => Seq.fill( numColumns )( Random.nextFloat() ) );
            val df2 = createDataFrame( data_f, schema_f );
            logger.debug(s"Created a dataframe with float values in it with (${df2.count} rows)");

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df2.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            table_size = get_table_size( tableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );

            // Delete all records from the table for the next test
            delete_all_records( tableName );
            
            // Test inserting int into float
            // --------------------------------
            logger.debug(s"Test inserting integer into float");

            // Generate appropriate data
            val data_i = (1 to numRows).map(_ => Seq.fill( numColumns )( Random.nextInt() ) );
            
            // Generate the schema with a float type
            val schema_i = StructType( StructField( "float_col", IntegerType, true ) :: Nil );
            
            val df3 = createDataFrame( data_i, schema_i );
            logger.debug(s"Created a dataframe with integer values in it with (${df3.count} rows)");

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df3.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            table_size = get_table_size( tableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );

            // Delete all records from the table for the next test
            delete_all_records( tableName );
            
            // Test inserting long into float
            // --------------------------------
            logger.debug(s"Test inserting long into float");

            // Generate the data and the schema with a long type
            val data_l = (1 to numRows).map(_ => Seq.fill( numColumns )( Random.nextLong() ) );
            val schema_l = StructType( StructField( "float_col", LongType, true ) :: Nil );
            
            val df4 = createDataFrame( data_l, schema_l );
            logger.debug(s"Created a dataframe with long values in it with (${df4.count} rows)");

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df4.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            table_size = get_table_size( tableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );

            // Delete all records from the table for the next test
            delete_all_records( tableName );
            
            // Test inserting string into float
            // --------------------------------
            logger.debug(s"Test inserting string into float");

            // Generate the data and the schema with a long type
            val data_s = (1 to numRows).map(_ => Seq.fill( numColumns )( "42.42" ) );
            val schema_s = StructType( StructField( "float_col", StringType, true ) :: Nil );
            
            val df5 = createDataFrame( data_s, schema_s );
            logger.debug(s"Created a dataframe with long values in it with (${df5.count} rows)");

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df5.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            table_size = get_table_size( tableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );
        }  // end test for KECO-1355

        
        /**
         * Tests for `table.truncate_to_size`
         */
        test(s"""$package_description KECO-1371: `table.truncate_to_size`
             | should work""".stripMargin.replaceAll("\n", "") ) {

            // We need a table with a float column
            val sort_col_name = "i";
            val char1_name    = "char1";
            val char2_name    = "char2";
            val char4_name    = "char4";
            val char8_name    = "char8";
            val char16_name   = "char16";
            val char32_name   = "char32";
            val char64_name   = "char64";
            val char128_name  = "char128";
            val char256_name  = "char256";
            val str_name      = "str";
            val bytes_name    = "bytes";
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( sort_col_name,
                                        classOf[java.lang.Integer] );
            columns += new Type.Column( char1_name,
                                        classOf[java.lang.String],
                                        ColumnProperty.CHAR1 );
            columns += new Type.Column( char2_name,
                                        classOf[java.lang.String],
                                        ColumnProperty.CHAR2 );
            columns += new Type.Column( char4_name,
                                        classOf[java.lang.String],
                                        ColumnProperty.CHAR4 );
            columns += new Type.Column( char8_name,
                                        classOf[java.lang.String],
                                        ColumnProperty.CHAR8 );
            columns += new Type.Column( char16_name,
                                        classOf[java.lang.String],
                                        ColumnProperty.CHAR16 );
            columns += new Type.Column( char32_name,
                                        classOf[java.lang.String],
                                        ColumnProperty.CHAR32 );
            columns += new Type.Column( char64_name,
                                        classOf[java.lang.String],
                                        ColumnProperty.CHAR64 );
            columns += new Type.Column( char128_name,
                                        classOf[java.lang.String],
                                        ColumnProperty.CHAR128 );
            columns += new Type.Column( char256_name,
                                        classOf[java.lang.String],
                                        ColumnProperty.CHAR256 );
            columns += new Type.Column( str_name,
                                        classOf[java.lang.String] );
            columns += new Type.Column( bytes_name,
                                        classOf[java.nio.ByteBuffer] );

            // Create the table
            val tableName = s"keco_1371_${package_description}";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"           ) = "false";
            options( "table.name"             ) = tableName;
            options( "table.truncate_to_size" ) = "true";

            // Get some test data with a bad row
            // ---------------------------------
            val data = Seq(
                            // Precisely N characters (for charN values)
                            Row( 1, "a", "bb", "cccc", "ccccdddd", "ccccddddccccdddd",
                                 "This is 32 characters long......",
                                 "This is 64 characters long......This is 64 characters long......",
                                 "This is 128 characters long.....This is 128 characters long.....This is 128 characters long.....This is 128 characters long.....",
                                 "This is 256 characters long.....This is 256 characters long.....This is 256 characters long.....This is 256 characters long.....This is 256 characters long.....This is 256 characters long.....This is 256 characters long.....This is 256 characters long.....",
                                 "Some random string value...........",
                                 "Some random bytes value..........." ),
                            // Less than N characters (for charN values)
                            Row( 2, "", "b", "cc", "ccdd", "ccccdddd",
                                 "This is less than 32 chars",
                                 "This is less than 64 characters...",
                                 "This is less than 128 characters...",
                                 "This is less than 256 characters long...",
                                 "Some random string value...........",
                                 "Some random bytes value..........." ),
                            // More than N characters (for charN values)
                            Row( 3, "aeeee", "bbeeeee", "cccceeeee", "ccccddddeeeeeeeeeee", "ccccddddccccddddeeeeeeeeeee",
                                 "The 33rd characters has a pipe..|extra extra extra",
                                 "The 65th character has a pipe; before then, it's periods........|extra extra extra",
                                 "The 129th character has a pipe; before then, it's periods.......................................................................|extra extra extra",
                                 "The 257th character has a pipe; before then, it's periods.......................................................................................................................................................................................................|extra extra extra",
                                 "Some random string value...........| this should make it in, too",
                                 "Some random bytes value...........| this should make it in, too" ) );
            // The expected values have only two rows (the middle one
            // having been discarded)
            val expected_values = Seq( Map( sort_col_name -> 1,
                                            char1_name    -> "a",
                                            char2_name    -> "bb",
                                            char4_name    -> "cccc",
                                            char8_name    -> "ccccdddd",
                                            char16_name   -> "ccccddddccccdddd",
                                            char32_name   -> "This is 32 characters long......",
                                            char64_name   -> "This is 64 characters long......This is 64 characters long......",
                                            char128_name  -> "This is 128 characters long.....This is 128 characters long.....This is 128 characters long.....This is 128 characters long.....",
                                            char256_name  -> "This is 256 characters long.....This is 256 characters long.....This is 256 characters long.....This is 256 characters long.....This is 256 characters long.....This is 256 characters long.....This is 256 characters long.....This is 256 characters long.....",
                                            str_name      -> "Some random string value...........",
                                            bytes_name    -> ByteBuffer.wrap( "Some random bytes value...........".getBytes() ) ),
                                       Map( sort_col_name  -> 2,
                                            char1_name    -> "",
                                            char2_name    -> "b",
                                            char4_name    -> "cc",
                                            char8_name    -> "ccdd",
                                            char16_name   -> "ccccdddd",
                                            char32_name   -> "This is less than 32 chars",
                                            char64_name   -> "This is less than 64 characters...",
                                            char128_name  -> "This is less than 128 characters...",
                                            char256_name  -> "This is less than 256 characters long...",
                                            str_name      -> "Some random string value...........",
                                            bytes_name    -> ByteBuffer.wrap( "Some random bytes value...........".getBytes() ) ),
                                       Map( sort_col_name  -> 3,
                                            char1_name    -> "a",
                                            char2_name    -> "bb",
                                            char4_name    -> "cccc",
                                            char8_name    -> "ccccdddd",
                                            char16_name   -> "ccccddddccccdddd",
                                            char32_name   -> "The 33rd characters has a pipe..",
                                            char64_name   -> "The 65th character has a pipe; before then, it's periods........",
                                            char128_name  -> "The 129th character has a pipe; before then, it's periods.......................................................................",
                                            char256_name  -> "The 257th character has a pipe; before then, it's periods.......................................................................................................................................................................................................",
                                            str_name      -> "Some random string value...........| this should make it in, too",
                                            bytes_name    -> ByteBuffer.wrap( "Some random bytes value...........| this should make it in, too".getBytes() ) )
                                       );

            // Generate the appropriate schema create the test data
            val schema = StructType( StructField( sort_col_name, IntegerType, true ) ::
                                     StructField( char1_name,    StringType,  true ) ::
                                     StructField( char2_name,    StringType,  true ) ::
                                     StructField( char4_name,    StringType,  true ) ::
                                     StructField( char8_name,    StringType,  true ) ::
                                     StructField( char16_name,   StringType,  true ) ::
                                     StructField( char32_name,   StringType,  true ) ::
                                     StructField( char64_name,   StringType,  true ) ::
                                     StructField( char128_name,  StringType,  true ) ::
                                     StructField( char256_name,  StringType,  true ) ::
                                     StructField( str_name,      StringType,  true ) ::
                                     StructField( bytes_name,    StringType,  true ) :: Nil );
            val df = createDataFrame( data, schema );
            logger.debug(s"Created a dataframe with a bad row in it (${df.count} rows)");
            

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            var table_size = get_table_size( tableName );
            assert( (table_size == expected_values.length),
                    s"Table size ($table_size) should be ${expected_values.length}" );

            // Check correctness of the data
            val columns_to_compare = sort_col_name :: char1_name :: char2_name ::
                                   char4_name :: char8_name :: char16_name ::
                                   char32_name :: char64_name :: char128_name ::
                                   char256_name :: str_name :: bytes_name :: Nil;
            compare_table_data( tableName, columns_to_compare, sort_col_name, expected_values );
        }  // end test for KECO-1371



        /**
         * Test for egress honoring filters.
         */
        test(s"""$package_description KECO-1402: Egress needs to honor any filters
             | passed to the connector""".stripMargin.replaceAll("\n", "") ) {

            // Create a table (but clear any pre-existing ones)
            val tableName = s"keco_1402_${package_description}";
            logger.debug( s"Table name '${tableName}'" );
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( "x", classOf[java.lang.Integer], ColumnProperty.NULLABLE );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );
            // createKineticaTableOneIntNullableColumn( tableName, None, 0 );

            // // Need to mark the table name for post-test clean-up
            // mark_table_for_deletion_at_test_end( tableName );

            // Create some test data for a single int column where the first
            // row has a value of 0 and it increases by one per row
            val numTotalRecords = 100000;
            val records = for (i <- 0 until numTotalRecords) yield (i :: Nil);

            // Generate the schema with a integer type
            val schema = StructType( StructField( "x", IntegerType, true ) :: Nil );
            val df_in = createDataFrame( records, schema );
            logger.debug(s"Created a dataframe with sequential integers in it (${df_in.count} rows)");
            
            // Get the appropriate options for fetching data
            var options = get_default_spark_connector_options();
            options( "table.name"           ) = tableName;
            options( "spark.num_partitions" ) = "8";

            // Write the data to the table
            df_in.write.format( package_to_test ).options( options ).save();
            logger.debug( s"Writing to table ${tableName} via the connector" );

            // Read from the table with an expression
            val expectedNumRecords = 70000;
            val filter_expression = s"x < ${expectedNumRecords}";
            logger.debug( s"Reading from table ${tableName} via the connector" );
            val fetched_records = m_sparkSession.sqlContext.read.format( package_to_test ).options( options ).load().filter( filter_expression );
            logger.debug( s"------------Read from table ${tableName} via the connector; got ${fetched_records.count} records" );

            // Check that the table size is correct
            assert( (fetched_records.count == expectedNumRecords),
                    s"Should have fetched ${expectedNumRecords}, got ${fetched_records.count}" );
        }  // end test for KECO-1402


        /**
         * Test for ingesting an empty dataframe (should not crash)
         */
        test(s"""$package_description KECO-1405: Empty dataframe should not
             | cause any crash or exception to be thrown; should just log
             | warning""".stripMargin.replaceAll("\n", "") ) {

            // Get a table name and clear any pre-existing ones
            val tableName = s"keco_1405_${package_description}";
            clear_table( tableName );
            logger.debug( s"Table name '${tableName}'" );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( tableName );
            
            // Test Case: Empty dataframe with no DDL
            // --------------------------------------
            logger.info("Test Case: Empty dataframe with no DDL");
            // Create an empty dataframe
            val empty_df = m_sparkSession.sqlContext.emptyDataFrame
            logger.debug(s"Created an empty dataframe (${empty_df.count} rows)");

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "true";
            options( "table.name"                 ) = tableName;

            // Try to write to the table
            try{
                logger.debug( s"Trying to write the empty dataframe (without schema)" );
                empty_df.write.format( package_to_test ).options( options ).save();
                logger.debug( s"Trying to write the empty dataframe without schema did not crash!" );
            } catch {
                case e: com.kinetica.spark.util.table.KineticaException => {
                    logger.debug( s"Got KINETICA exception {}", e );
                    assert( (false), s"Empty datafame without schema should not cause an exception" );
                }
                case e2: java.lang.RuntimeException => {
                    logger.debug( s"Got RUNTIME exception {}", e2 );
                    assert( (false), s"Empty datafame without schema should not cause an exception" );
                }
            }

            // Should not have created a table with the name
            assert( (does_table_exist( tableName ) == false), s"Empty datafame without schema should not create a table" );

            
            // Test Case: Empty dataframe with DDL
            // --------------------------------------
            logger.info("Test Case: Empty dataframe with DDL");

            // Create an empty dataframe with a DDL
            val schema_s = StructType( StructField( "string_col", StringType, true ) :: Nil );
            val empty_df_with_ddl = m_sparkSession.sqlContext.createDataFrame( m_sparkSession.sparkContext.emptyRDD[Row],
                                                                               schema_s );
            logger.debug(s"Created an empty dataframe with a schema(${empty_df_with_ddl.count} rows)");

            // Try to write to the table
            try{
                logger.debug( s"Trying to write the empty dataframe with a schema" );
                empty_df_with_ddl.write.format( package_to_test ).options( options ).save();
                logger.debug( s"Trying to write the empty dataframe (with schema) did not crash!" );
            } catch {
                case e: com.kinetica.spark.util.table.KineticaException => {
                    logger.debug( s"Got KINETICA exception {}", e );
                    assert( (false), s"Empty datafame with schema should not cause an exception" );
                }
                case e2: java.lang.RuntimeException => {
                    logger.debug( s"Got RUNTIME exception {}", e2 );
                    assert( (false), s"Empty datafame with schema should not cause an exception" );
                }
            }

            // Should not have created a table with the name
            assert( (does_table_exist( tableName ) == true), s"Empty datafame with schema should create a table" );
            val tableSize = get_table_size( tableName );
            assert( (tableSize == 0), s"Empty datafame with schema should create an empty table; got size ${tableSize}" );
        }  // end test for KECO-1405


        
        /**
         * Test for fail-fase mode actually failing
         */
        test(s"""$package_description KECO-1415: NULL value for non-nullable
             | column should fail for failfast
             | mode""".stripMargin.replaceAll("\n", "") ) {

            // We need a table with a float column
            val sort_col_name = "i";
            val str_col_name = "str_col";
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( sort_col_name, classOf[java.lang.Integer] );
            columns += new Type.Column( str_col_name,
                                        classOf[java.lang.String],
                                        ColumnProperty.PRIMARY_KEY,
                                        ColumnProperty.DATE );

            // Create the table
            val tableName = s"keco_1415_${package_description}";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create" ) = "false";
            options( "table.name"   ) = tableName;

            // Get some test data with a bad row
            // ---------------------------------
            val data = Seq( Row( 1, "1960-01-01" ),
                            Row( 2, "1965-03-03 12:00:00" ),
                            // Bad row: can't have a null!
                            Row( 3, null ),
                            Row( 4, "1970-12-13" ) );

            // Generate the appropriate schema create the test data
            val schema = StructType( StructField( sort_col_name, IntegerType, true ) ::
                                     StructField( str_col_name,  StringType,  true ) :: Nil );
            val df = createDataFrame( data, schema );
            logger.debug(s"Created a dataframe with a bad row in it (${df.count} rows)");
            


            // Test Case 1: Write to the table with fail-fast mode
            logger.info( s"Test Case 1: Write to the table with fail-fast mode" );
            options( "table.fail_on_errors" ) = "true";
            logger.debug( s"Writing to table ${tableName} via the connector" );

            try {
                df.write.format( package_to_test ).options( options ).save();
                assert( (false), s"Nul value for non-nullable column should fail for failfast mode" );
            } catch {
                case e: com.kinetica.spark.util.table.KineticaException => {
                    logger.debug( s"Got KINETICA exception {}", e );
                    assert( (true), s"Nul value for non-nullable column should fail for failfast mode" );
                }
                case e2: java.lang.RuntimeException => {
                    logger.debug( s"Got RUNTIME exception {}", e2 );
                    assert( (true), s"Nul value for non-nullable column should fail for failfast mode" );
                }
            }

        }  // end test for KECO-1415



        
        /**
         * Test for data with bad format being handled properly
         */
        test(s"""$package_description KECO-1415: Date column with non-conforming format
             | should be handled properly""".stripMargin.replaceAll("\n", "") ) {

            // We need a table with a float column
            val sort_col_name = "i";
            val str_col_name = "str_col";
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( sort_col_name, classOf[java.lang.Integer] );
            columns += new Type.Column( str_col_name,
                                        classOf[java.lang.String],
                                        ColumnProperty.PRIMARY_KEY,
                                        ColumnProperty.DATE );

            // Create the table
            val tableName = s"keco_1415_${package_description}";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create" ) = "false";
            options( "table.name"   ) = tableName;

            // Get some test data with a bad row
            // ---------------------------------
            val data = Seq( Row( 1, "1960-01-01" ),
                            // Date with time component
                            Row( 2, "1965-03-03 12:00:00" ),
                            // Date with time component
                            Row( 3, "1970-12-13T13:13:13.123" ) );
            // The expected values have only two rows (the middle one
            // having been discarded)
            val expected_values = Seq( Map( sort_col_name -> 1,
                                            str_col_name -> "1960-01-01" ),
                                       Map( sort_col_name -> 2,
                                            str_col_name -> "1965-03-03" ),
                                       Map( sort_col_name -> 3,
                                            str_col_name -> "1970-12-13" )
                                       );

            // Generate the appropriate schema create the test data
            val schema = StructType( StructField( sort_col_name, IntegerType, true ) ::
                                     StructField( str_col_name,  StringType,  true ) :: Nil );
            val df = createDataFrame( data, schema );
            logger.debug(s"Created a dataframe with a bad row in it (${df.count} rows)");
            

            // Test Case 2: Write to the table with graceful failure
            logger.info( s"Test Case 2: Write to the table with graceful failure mode" );
            options( "table.fail_on_errors" ) = "false";
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            var table_size = get_table_size( tableName );
            assert( (table_size == expected_values.length),
                    s"Table size ($table_size) should be ${expected_values.length}" );

            // Check correctness of the data
            val columns_to_compare = sort_col_name :: str_col_name :: Nil;
            compare_table_data( tableName, columns_to_compare, sort_col_name, expected_values );
        }  // end test for KECO-1415



        
        /**
         * Test for egressing string time values.
         */
        test(s"""$package_description KECO-1419: Time-type data should not be
             | prepended with the current date""".stripMargin.replaceAll("\n", "") ) {

            // Create a table type with date, time, datetime columns
            val sort_col_name = "i";
            val time_col_name     = "time";
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( sort_col_name, classOf[java.lang.Integer] );
            columns += new Type.Column( time_col_name, classOf[java.lang.String],
                                        ColumnProperty.NULLABLE,
                                        ColumnProperty.TIME );

            // Create the table (but clear any pre-existing ones)
            val tableName = s"keco_1419_string_time_${package_description}";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Create some test data
            // Set the default timezone appropriately
            val timeZone = "GMT+0600";
            TimeZone.setDefault( TimeZone.getTimeZone( timeZone ) );
            // The test data don't conform to the Kinetica format, for the most part
            val data = Seq( Row( 0, "09:50:00.005-04:00" ),
                            Row( 1, "09:50:00.004+08:00" ),
                            Row( 2, "09:50:00.123456"    ) );
            // The expected values would be of Kinetica format and be local to the specified timezone
            val expected_values_via_native_client = Seq( Map( "time" ->"19:50:00.005" ),
                                                         Map( "time" ->"07:50:00.004" ),
                                                         Map( "time" -> "09:50:00.123" ) );
            val expected_values_via_jdbc = Seq( Map( "time" ->"19:50:00" ),
                                                Map( "time" ->"07:50:00" ),
                                                Map( "time" -> "09:50:00" ) );

            // Generate the appropriate schema
            val schema = StructType( StructField( sort_col_name, IntegerType, true ) ::
                                     StructField( time_col_name, StringType, true ) :: Nil );
            val rddRows = m_sparkSession.sparkContext.parallelize( data );
            // Generate the dataframe from the RDD
            val df = m_sparkSession.sqlContext.createDataFrame( rddRows, schema );
            logger.debug(s"Created a dataframe with various time & date values in it with (${df.count} rows)");

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"          ) = "false";
            options( "table.name"            ) = tableName;
            options( "ingester.use_timezone" ) = timeZone;

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            val table_size = get_table_size( tableName );
            assert( (table_size == df.count), s"Table size ($table_size) should be ${df.count}" );

            // Get the data out using the connecotr via the Java API (when no filtering is necessary)
            // Get the appropriate options
            var egress_options = get_default_spark_connector_options();
            egress_options( "table.name" ) = tableName;
            val records_native = m_sparkSession.sqlContext.read.format( package_to_test )
                                   .options( options ).load();
            val fetched_records_native = records_native.orderBy( sort_col_name ).collect();

            // Check for data correctness
            for ( i <- 0 until 3 ) {
                val expected = expected_values_via_native_client( i )( time_col_name );
                val actual   = fetched_records_native( i ).getAs[String]( time_col_name  );
                assert( (expected == actual),
                        s"Fetched value (via the Java API) of record #$i '$actual' should be '$expected'" );
            }


            // Get the data out using the connecotr via the JDBC connector (when filtering IS necessary)
            val filter_expression = s"i < 1000";
            val records_jdbc = m_sparkSession.sqlContext.read
                                 .format( package_to_test )
                                 .options( options ).load()
                                 .filter( filter_expression );
            val fetched_records_jdbc = records_jdbc.orderBy( sort_col_name ).collect();

            // Check for data correctness
            for ( i <- 0 until 3 ) {
                val expected = expected_values_via_jdbc( i )( time_col_name );
                val actual   = fetched_records_jdbc( i ).getAs[String]( time_col_name  );
                assert( (expected == actual),
                        s"Fetched value (via the JDBC connector) of record #$i '$actual' should be '$expected'" );
            }
        }  // end test for KECO-1419


        
        /**
         * Test for graceful failures not throwing exceptions
         */
        test(s"""$package_description KECO-1457: Graceful failure mode should
             | not throw exceptions""".stripMargin.replaceAll("\n", "") ) {

            // We need a table with a float column
            val int_col_name = "int_col";
            val str_col_name1 = "str_col1";
            val str_col_name2 = "str_col2";
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( int_col_name,
                                        classOf[java.lang.Integer],
                                        ColumnProperty.NULLABLE );
            columns += new Type.Column( str_col_name1,
                                        classOf[java.lang.String],
                                        ColumnProperty.NULLABLE );
            columns += new Type.Column( str_col_name2,
                                        classOf[java.lang.String],
                                        ColumnProperty.NULLABLE );

            // Create the table
            val tableName = s"keco_1457_${package_description}";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // // Need to mark the table name for post-test clean-up
            // mark_table_for_deletion_at_test_end( tableName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "false";
            options( "table.name"                 ) = tableName;

            // Get some test data with a bad row
            // ---------------------------------
            // Note that the column with type int in the table is a string in
            // the schema here; also, the last column is a string in the table,
            // but an int here.  The connector should be able to parse strings
            // into ints and ints into strings; BUT the second row is bad because
            // "ABCD" cannot be parsed as an integer.
            val data = Seq( Row( "1", "defg", 42 ),
                            // Bad row: the first value can't be parsed as int
                            Row( "ABCD", "DEFG", null ),
                            Row( "3", "asdf", 24    ) );
            // The expected values have only two rows (the middle one
            // having been discarded)
            val expected_values = Seq( Map( int_col_name  -> 1,
                                            str_col_name1 -> "defg",
                                            str_col_name2 -> "42" ),
                                       Map( int_col_name  -> 3,
                                            str_col_name1 -> "asdf",
                                            str_col_name2 -> "24" )
                                       );

            // Generate the appropriate schema create the test data
            // Note that the column with type int in the table is a string in
            // the schema here; also, the last column is a string in the table,
            // but an int here
            val schema = StructType( StructField( int_col_name,  StringType, true ) ::
                                     StructField( str_col_name1, StringType,  true ) ::
                                     StructField( str_col_name2, IntegerType,  true ) :: Nil );
            val df = createDataFrame( data, schema );
            logger.debug(s"Created a dataframe with a bad row in it (${df.count} rows)");
            

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            var table_size = get_table_size( tableName );
            assert( (table_size == expected_values.length),
                    s"Table size ($table_size) should be ${expected_values.length}" );

            // Check correctness of the data
            val columns_to_compare = int_col_name :: str_col_name1 :: str_col_name2 :: Nil;
            compare_table_data( tableName, columns_to_compare, int_col_name, expected_values );
        }  // end test for KECO-1457


        /**
         * Test for restricted users
         */
        test(s"""$package_description KECO-1481: Users with only table
             | permissions should be able to ingest and egress without
             | issues""".stripMargin.replaceAll("\n", "") ) {

            // This test can only be run if certain criteria are met
            val sys_properties = m_gpudb.showSystemProperties( new ShowSystemPropertiesRequest() )
                                        .getPropertyMap();

            val require_authentication_flag       = "conf.require_authentication";
            val require_authentication_flag_value = sys_properties.get( require_authentication_flag );
            logger.debug( s"Checking that authentication is required: '$require_authentication_flag' = '$require_authentication_flag_value'" );
            assume( require_authentication_flag_value == "TRUE" );
            logger.debug( s"Authorization related settings are found to be as expected" );

            // Make sure that the 'admin' username is passed to the test harness
            // so that internal users can be created
            logger.info( s"Make sure that the user (test runner) passed in the 'admin' username and password (necessary for creating internal users)" );
            assume( m_username == "admin" );
            
            // Create a type
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( "i", classOf[java.lang.Integer] );
            columns += new Type.Column( "d", classOf[java.lang.Double] );

            // Create a table
            val tableName = s"keco_1481_${package_description}";
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Create a user and a password
            // ----------------------------
            // Username can't be pre-existing; in case the test was run before
            val ingest_username = "spark_test_user_ingest_" + getRandomKineticaUsername( 10 );
            val ingest_password = "spark_test_password";
            logger.debug( s"Trying to create internal user '$ingest_username'" );
            m_gpudb.createUserInternal( ingest_username, ingest_password, null );
            logger.debug( s"Created internal user '$ingest_username'" );

            // Grant the user admin permissions on the table
            logger.debug( s"Granting user '$ingest_username' admin permission to table '$tableName'" );
            m_gpudb.grantPermissionTable( ingest_username,
                                          GrantPermissionTableRequest.Permission.TABLE_ADMIN,
                                          tableName,
                                          null, null );

            // Perform the ingest test
            // -----------------------
            // Get the proper ingestion options with the username and password
            var options = get_default_spark_connector_options();
            options( "table.create"      ) = "false";
            options( "table.name"        ) = tableName;
            options( "database.username" ) = ingest_username;
            options( "database.password" ) = ingest_password;
            
            // Generate some data and the appropriate schema
            val data = Seq( Row( 0, 1.2 ), Row( 1, 2.4 ), Row( 2, 3.5 ), Row( 3, 4.2 ) );
            val expected_values = Seq( Map( "i" -> 0, "d" -> 1.2 ),
                                       Map( "i" -> 1, "d" -> 2.4 ),
                                       Map( "i" -> 2, "d" -> 3.5 ),
                                       Map( "i" -> 3, "d" -> 4.2 ) );
            val schema = StructType( StructField( "i", IntegerType, true ) ::
                                     StructField( "d", DoubleType, true ) :: Nil );
            val df = createDataFrame( data, schema );
            logger.debug(s"Created a dataframe with some values in it with (${df.count} rows)");

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            var table_size = get_table_size( tableName );
            assert( (table_size == expected_values.length), s"Table size ($table_size) should be ${expected_values.length}" );

            // Check the data
            val columns_to_compare = "d" :: Nil;
            compare_table_data( tableName, columns_to_compare, "i", expected_values );

            // Egress test (with a different user)
            // -----------------------------------
            // Username can't be pre-existing; in case the test was run before
            val egress_username = "spark_test_user_egress_" + getRandomKineticaUsername( 10 );
            val egress_password = "spark_test_password";
            logger.debug( s"Trying to create internal user for egress test '$egress_username'" );
            m_gpudb.createUserInternal( egress_username, egress_password, null );
            logger.debug( s"Created internal user '$egress_username'" );

            // Grant the user admin permissions on the table
            logger.debug( s"Granting user '$egress_username' admin permission to table '$tableName'" );
            m_gpudb.grantPermissionTable( egress_username,
                                          GrantPermissionTableRequest.Permission.TABLE_READ,
                                          tableName,
                                          null, null );

            // Get the proper egress options with the username and password
            var egress_options = get_default_spark_connector_options();
            egress_options( "table.create"      ) = "false";
            egress_options( "table.name"        ) = tableName;
            egress_options( "database.username" ) = egress_username;
            egress_options( "database.password" ) = egress_password;
            
            // Check egress
            val df_egress = m_sparkSession.sqlContext.read.format( package_to_test )
                                             .options( egress_options ).load();
            
        }  // end test #1 for KECO-1481


        
        /**
         * Test for ingesting into a sharded table.
         */
        test(s"""$package_description KECO-1503: Ingestion into a sharded table
             | should work without issues""".stripMargin.replaceAll("\n", "") ) {

            // We need a table with a float column
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( "i",
                                        classOf[java.lang.Integer],
                                        ColumnProperty.SHARD_KEY );

            // Create the table
            val tableName = s"keco_1503_sharded_${package_description}";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create" ) = "false";
            options( "table.name"   ) = tableName;

            logger.debug(s"Test inserting into sharded table");

            // Create some data
            val numRows = 100;
            val numColumns = 1;
            val data = (1 to numRows).map(_ => Seq.fill( numColumns )( Random.nextInt() ) );

            // Generate the schema with a double type
            val schema = StructType( StructField( "i", IntegerType, true ) :: Nil );
            
            val df = createDataFrame( data, schema );
            logger.debug(s"Created a dataframe (${df.count} rows)");

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            var table_size = get_table_size( tableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );
        }   // end test #1 for KECO-1503


        /**
         * Test for ingesting into a replicated table.
         */
        test(s"""$package_description KECO-1503: Ingestion into a replicated table
             | should work without issues""".stripMargin.replaceAll("\n", "") ) {

            // We need a table with a float column
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( "i",
                                        classOf[java.lang.Integer] );

            // Create the table
            val tableName = s"keco_1503_replicated_${package_description}";
            logger.debug( s"Table name '${tableName}'" );
            createReplicatedKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create" ) = "false";
            options( "table.name"   ) = tableName;

            logger.debug(s"Test inserting into replicated table");

            // Create some data
            val numRows = 100;
            val numColumns = 1;
            val data = (1 to numRows).map(_ => Seq.fill( numColumns )( Random.nextInt() ) );

            // Generate the schema with a double type
            val schema = StructType( StructField( "i", IntegerType, true ) :: Nil );
            
            val df = createDataFrame( data, schema );
            logger.debug(s"Created a dataframe (${df.count} rows)");

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            var table_size = get_table_size( tableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );
        }   // end test #2 for KECO-1503
        
        
    }   // end tests for bugFixes



}   // end trait SparkConnectorBugFixes



/**
 *  Test bug fixes using the DataSource v1 package.
 */
class TestBugFixes_V1
    extends SparkConnectorTestBase
            with SparkConnectorBugFixes {

    override val m_package_descr   = m_v1_package_descr;
    override val m_package_to_test = m_v1_package;

    // Run the tests
    testsFor( bugFixes( m_package_to_test, m_package_descr ) );
    
}  // TestBugFixes_V1



/**
 *  Test bug fixes using the DataSource v2 package.
 */
class TestBugFixes_V2
    extends SparkConnectorTestBase
            with SparkConnectorBugFixes {

    override val m_package_descr   = m_v2_package_descr;
    override val m_package_to_test = m_v2_package;

    // Run the tests
    testsFor( bugFixes( m_package_to_test, m_package_descr ) );

}  // end TestBugFixes_V2
