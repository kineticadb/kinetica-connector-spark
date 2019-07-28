package com.kinetica.spark;

import com.gpudb.ColumnProperty;
import com.gpudb.Record;
import com.gpudb.Type;

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
            val tableName = "keco_1396_long_timestamp";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( tableName );

            // // Set the default timezone appropriately
            val timeZone = "GMT+0600";
            // TimeZone.setDefault( TimeZone.getTimeZone( timeZone ) );

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
            val columns_to_fetch = ts_col_name :: Nil;
            val fetched_records = get_records_by_column( tableName,
                                                         columns_to_fetch,
                                                         0, 100,
                                                         Option.apply(sort_col_name),
                                                         Option.apply("ascending") );
            for ( i <- 0 until data.length ) {
                var expected = expected_values(i)( ts_col_name );
                var actual   = fetched_records.get(i).get( ts_col_name );
                assert( (expected == actual),
                        s"Fetched value of record #$i '$actual' should be '$expected' for '$ts_col_name'" );
            }
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
            val tableName = "keco_1396_string_date_time_datetime";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( tableName );

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
            assert( (table_size == df.count), s"Table size ($table_size) should be $df.count" );

            // Check the data
            val columns_to_fetch = col1_name :: col2_name :: col3_name :: Nil;
            val fetched_records = get_records_by_column( tableName,
                                                         columns_to_fetch,
                                                         0, 100,
                                                         Option.apply(sort_col_name),
                                                         Option.apply("ascending") );
            for ( i <- 0 until 3 ) {
                var expected = expected_values(i)("date");
                var actual   = fetched_records.get(i).get("date");
                assert( (expected == actual),
                        s"Fetched value of record #$i '$actual' should be '$expected' for 'date'" );
                expected = expected_values(i)("time");
                actual   = fetched_records.get(i).get("time");
                assert( (expected == actual),
                        s"Fetched value of record #$i '$actual' should be '$expected' for 'time'" );
                expected = expected_values(i)("datetime");
                actual   = fetched_records.get(i).get("datetime");
                assert( (expected == actual),
                        s"Fetched value of record #$i '$actual' should be '$expected' for 'datetime'" );
            }
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
            val tableName  = "keco_1396_date_time_datetime_timestamp";
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
                    assert( (false), s"Re-ingesting data obtained by egree via the connector from Kinetica should not fail (without filters)" );
                }
                case e2: java.lang.RuntimeException => {
                    logger.debug( s"Got RUNTIME exception {}", e2 );
                    assert( (false), s"Re-ingesting data obtained by egree via the connector from Kinetica should not fail (without filters)" );
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
            val tableName = "keco_1355";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( tableName );

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
         * Test for ingesting an empty dataframe (should not crash)
         */
        test(s"""$package_description KECO-1405: Empty dataframe should not
             | cause any crash or exception to be thrown; should just log
             | warning""".stripMargin.replaceAll("\n", "") ) {

            // Get a table name and clear any pre-existing ones
            val tableName = "keco_1405";
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
         * Test for egress honoring filters.
         */
        test(s"""$package_description KECO-1402: Egress needs to honor any filters
             | passed to the connector""".stripMargin.replaceAll("\n", "") ) {

            // Create a table (but clear any pre-existing ones)
            val tableName = "keco_1402";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableOneIntNullableColumn( tableName, None, 0 );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( tableName );

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
            val tableName = "keco_1419_string_time";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableWithGivenColumns( tableName, None, columns, 0 );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( tableName );

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
            assert( (table_size == df.count), s"Table size ($table_size) should be $df.count" );

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
