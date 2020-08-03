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
trait SparkConnectorNewFeatures
    extends SparkConnectorTestFixture { this: FunSuite =>

    /**
     * Tests for various new features.
     */
    def newFeatures( package_to_test: String, package_description: String ) {


        /**
         * Test for egressing with offset and limit; within the table's range
         */
        test(s"""$package_description KECO-1611: Case 1 -- Use offset and limit for egressing
             | from Kinetica""".stripMargin.replaceAll("\n", "") ) {

            logger.info( s"""$package_description KECO-1611: Case 1 -- Use offset and limit for egressing
             | from Kinetica""".stripMargin.replaceAll("\n", "")  );

            // This test fetches some data from the table, and ensures that
            // the offset and limit were honored

            // Create a table type with date, time, datetime columns
            val col_name = "i";
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( col_name, classOf[java.lang.Integer] );

            // Create the table (but clear any pre-existing ones)
            val initialTableName  = s"keco_1611_offset_limit_${package_description}";
            logger.debug( s"Table name '${initialTableName}'" );
            val tableName = createKineticaTableWithGivenColumns( initialTableName, columns, 0 );

            // Generate some test data
            // -----------------------
            val numRecords = 1000;
            logger.debug(s"Inserting $numRecords sequential integers");
            val random_options = immutable.Map[String,
                                               java.util.Map[String, java.lang.Double]](
                                    col_name ->
                                    immutable.Map[String,
                                                  java.lang.Double]("min" -> 1,
                                                                    "max" -> numRecords,
                                                                    "interval" -> 1).asJava ).asJava;
            m_gpudb.insertRecordsRandom( tableName, numRecords, random_options );

            // Check that the table size is correct
            val table_size = get_table_size( tableName );
            assert( (table_size == numRecords), s"Table size ($table_size) should be ${numRecords}" );

            // Test offset and limit for egress
            // --------------------------------

            val offset     = 100;
            val batch_size = 5;

            // Get the appropriate egress options
            var egress_options = get_default_spark_connector_options();
            egress_options( "table.name"    ) = tableName;
            egress_options( "egress.offset" ) = s"$offset";
            egress_options( "egress.limit"  ) = s"$batch_size";

            // TODO: The v2 package has an outstanding bug (KECO-1241) which
            //       prevents us from testing the Java API path (for the
            //       datasourcev2 package only!)
            if ( package_to_test != m_v2_package ) { // TODO: Remove the if filtering when KECO-1241 is fixed
                // Test the Java API path
                // ----------------------
                // Get the data out using the connecotr via the Java API (when no filtering is necessary)
                val fetched_records_native = m_sparkSession.sqlContext.read.format( package_to_test )
                    .options( egress_options ).load();
                logger.info( s"Extracted ${fetched_records_native.count} records from table ${tableName} via the connector (no filter --> use the Java API)" );

                assert( (fetched_records_native.count == batch_size),
                        s"Fetched dataframe size (${fetched_records_native.count}) should be ${numRecords}" );
            }


            // Test the JDBC connector path
            // ----------------------------
            // Get the data out using the connecotr via the JDBC connector (when filtering IS necessary)
            val filter_expression = s"i < 1000";
            val fetched_records_jdbc = m_sparkSession.sqlContext.read
                                          .format( package_to_test )
                                          .options( egress_options ).load()
                                          .filter( filter_expression );
            logger.info( s"Extracted ${fetched_records_jdbc.count} records from table ${tableName} via the connector (with filter --> use the JDBC connector)" );

            assert( (fetched_records_jdbc.count == batch_size),
                    s"Fetched dataframe size (${fetched_records_jdbc.count}) should be ${numRecords}" );
        }  // end test #1 for KECO-1611


        /**
         * Test for egressing with offset and limit, where a too large limit makes the offset
         * for some executors beyond the table size
         */
        test(s"""$package_description KECO-1611: Case 2 -- use offset and limit
             | for egressing from Kinetica with offset beyond the table size
             | for some executors""".stripMargin.replaceAll("\n", "") ) {

            logger.info( s"""$package_description KECO-1611: Case 2 -- use offset and limit
             | for egressing from Kinetica with offset beyond the table size
             | for some executors""".stripMargin.replaceAll("\n", "")  );

            // This test fetches some data from the table, and ensures that
            // the offset and limit were honored

            // Create a table type with date, time, datetime columns
            val col_name = "i";
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( col_name, classOf[java.lang.Integer] );

            // Create the table (but clear any pre-existing ones)
            val initialTableName  = s"keco_1611_offset_limit_${package_description}";
            logger.debug( s"Table name '${initialTableName}'" );
            val tableName = createKineticaTableWithGivenColumns( initialTableName, columns, 0 );

            // Generate some test data
            // -----------------------
            val numRecords = 1000;
            logger.debug(s"Inserting $numRecords sequential integers");
            val random_options = immutable.Map[String,
                                               java.util.Map[String, java.lang.Double]](
                                    col_name ->
                                    immutable.Map[String,
                                                  java.lang.Double]("min" -> 1,
                                                                    "max" -> numRecords,
                                                                    "interval" -> 1).asJava ).asJava;
            m_gpudb.insertRecordsRandom( tableName, numRecords, random_options );

            // Check that the table size is correct
            val table_size = get_table_size( tableName );
            assert( (table_size == numRecords), s"Table size ($table_size) should be ${numRecords}" );

            // Test offset and limit for egress
            // --------------------------------

            val offset     = 990;
            val batch_size = 100;
            val expected_num_records = 10;

            // Get the appropriate egress options
            var egress_options = get_default_spark_connector_options();
            egress_options( "table.name"    ) = tableName;
            egress_options( "egress.offset" ) = s"$offset";
            egress_options( "egress.limit"  ) = s"$batch_size";

            // TODO: The v2 package has an outstanding bug (KECO-1241) which
            //       prevents us from testing the Java API path (for the
            //       datasourcev2 package only!)
            if ( package_to_test != m_v2_package ) { // TODO: Remove the if filtering when KECO-1241 is fixed
                // Test the Java API path
                // ----------------------
                // Get the data out using the connecotr via the Java API (when no filtering is necessary)
                val fetched_records_native = m_sparkSession.sqlContext.read.format( package_to_test )
                    .options( egress_options ).load();
                logger.info( s"Extracted ${fetched_records_native.count} records from table ${tableName} via the connector (no filter --> use the Java API)" );

                assert( (fetched_records_native.count == expected_num_records),
                        s"Fetched dataframe size (${fetched_records_native.count}) should be ${expected_num_records}" );
            }


            // Test the JDBC connector path
            // ----------------------------
            // Get the data out using the connecotr via the JDBC connector (when filtering IS necessary)
            val filter_expression = s"i < 1000";
            val fetched_records_jdbc = m_sparkSession.sqlContext.read
                                          .format( package_to_test )
                                          .options( egress_options ).load()
                                          .filter( filter_expression );
            logger.info( s"Extracted ${fetched_records_jdbc.count} records from table ${tableName} via the connector (with filter --> use the JDBC connector)" );

            assert( (fetched_records_jdbc.count == expected_num_records),
                    s"Fetched dataframe size (${fetched_records_jdbc.count}) should be ${expected_num_records}" );
        }  // end test #2 for KECO-1611


        /**
         * Test for egressing with offset and limit where the offset is beyond
         * the table size
         */
        test(s"""$package_description KECO-1611: Case 3 -- use offset and limit
             | for egressing from Kinetica with offset beyond the table size;
             | should get zero records instead
             | failing""".stripMargin.replaceAll("\n", "") ) {

            logger.info(s"""$package_description KECO-1611: Case 3 -- use offset and limit
             | for egressing from Kinetica with offset beyond the table size;
             | should get zero records instead
             | failing""".stripMargin.replaceAll("\n", "") );

            // This test fetches some data from the table, and ensures that
            // the offset and limit were honored

            // Create a table type with date, time, datetime columns
            val col_name = "i";
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( col_name, classOf[java.lang.Integer] );

            // Create the table (but clear any pre-existing ones)
            val initialTableName  = s"keco_1611_offset_limit_${package_description}";
            logger.debug( s"Table name '${initialTableName}'" );
            val tableName = createKineticaTableWithGivenColumns( initialTableName, columns, 0 );

            // Generate some test data
            // -----------------------
            val numRecords = 1000;
            logger.debug(s"Inserting $numRecords sequential integers");
            val random_options = immutable.Map[String,
                                               java.util.Map[String, java.lang.Double]](
                                    col_name ->
                                    immutable.Map[String,
                                                  java.lang.Double]("min" -> 1,
                                                                    "max" -> numRecords,
                                                                    "interval" -> 1).asJava ).asJava;
            m_gpudb.insertRecordsRandom( tableName, numRecords, random_options );

            // Check that the table size is correct
            val table_size = get_table_size( tableName );
            assert( (table_size == numRecords), s"Table size ($table_size) should be ${numRecords}" );

            // Test offset and limit for egress
            // --------------------------------
            val offset     = 20000;
            val batch_size = 50;
            val expected_num_records = 0;

            // Get the appropriate egress options
            var egress_options = get_default_spark_connector_options();
            egress_options( "table.name"    ) = tableName;
            egress_options( "egress.offset" ) = s"$offset";
            egress_options( "egress.limit"  ) = s"$batch_size";

            // TODO: The v2 package has an outstanding bug (KECO-1241) which
            //       prevents us from testing the Java API path (for the
            //       datasourcev2 package only!)
            if ( package_to_test != m_v2_package ) { // TODO: Remove the if filtering when KECO-1241 is fixed
                // Test the Java API path
                // ----------------------
                // Get the data out using the connecotr via the Java API (when no filtering is necessary)
                val fetched_records_native = m_sparkSession.sqlContext.read.format( package_to_test )
                    .options( egress_options ).load();
                logger.info( s"Extracted ${fetched_records_native.count} records from table ${tableName} via the connector (no filter --> use the Java API)" );

                assert( (fetched_records_native.count == expected_num_records),
                        s"Fetched dataframe size (${fetched_records_native.count}) should be ${expected_num_records}" );
            }


            // Test the JDBC connector path
            // ----------------------------
            // Get the data out using the connecotr via the JDBC connector (when filtering IS necessary)
            val filter_expression = s"i < 1000";
            val fetched_records_jdbc = m_sparkSession.sqlContext.read
                                          .format( package_to_test )
                                          .options( egress_options ).load()
                                          .filter( filter_expression );
            logger.info( s"Extracted ${fetched_records_jdbc.count} records from table ${tableName} via the connector (with filter --> use the JDBC connector)" );

            assert( (fetched_records_jdbc.count == expected_num_records),
                    s"Fetched dataframe size (${fetched_records_jdbc.count}) should be ${expected_num_records}" );
        }  // end test #3 for KECO-1611


        /**
         * Test for egressing with offset and limit, where the offset is the same
         * as the table size
         */
        test(s"""$package_description KECO-1611: Case 4 -- use offset and limit
             | for egressing from Kinetica with offset same as the table size;
             | should get zero records instead
             | failing""".stripMargin.replaceAll("\n", "") ) {

            logger.info( s"""$package_description KECO-1611: Case 4 -- use offset and limit
             | for egressing from Kinetica with offset same as the table size;
             | should get zero records instead
             | failing""".stripMargin.replaceAll("\n", "" ) );

            // This test fetches some data from the table, and ensures that
            // the offset and limit were honored

            // Create a table type with date, time, datetime columns
            val col_name = "i";
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( col_name, classOf[java.lang.Integer] );

            // Create the table (but clear any pre-existing ones)
            val initialTableName  = s"keco_1611_offset_limit_${package_description}";
            logger.debug( s"Table name '${initialTableName}'" );
            val tableName = createKineticaTableWithGivenColumns( initialTableName, columns, 0 );

            // Generate some test data
            // -----------------------
            val numRecords = 1000;
            logger.debug(s"Inserting $numRecords sequential integers");
            val random_options = immutable.Map[String,
                                               java.util.Map[String, java.lang.Double]](
                                    col_name ->
                                    immutable.Map[String,
                                                  java.lang.Double]("min" -> 1,
                                                                    "max" -> numRecords,
                                                                    "interval" -> 1).asJava ).asJava;
            m_gpudb.insertRecordsRandom( tableName, numRecords, random_options );

            // Check that the table size is correct
            val table_size = get_table_size( tableName );
            assert( (table_size == numRecords), s"Table size ($table_size) should be ${numRecords}" );

            // Test offset and limit for egress
            // --------------------------------
            val offset     = 1000;
            val batch_size = 50;
            val expected_num_records = 0;

            // Get the appropriate egress options
            var egress_options = get_default_spark_connector_options();
            egress_options( "table.name"    ) = tableName;
            egress_options( "egress.offset" ) = s"$offset";
            egress_options( "egress.limit"  ) = s"$batch_size";

            // TODO: The v2 package has an outstanding bug (KECO-1241) which
            //       prevents us from testing the Java API path (for the
            //       datasourcev2 package only!)
            if ( package_to_test != m_v2_package ) { // TODO: Remove the if filtering when KECO-1241 is fixed
                // Test the Java API path
                // ----------------------
                // Get the data out using the connecotr via the Java API (when no filtering is necessary)
                val fetched_records_native = m_sparkSession.sqlContext.read.format( package_to_test )
                    .options( egress_options ).load();
                logger.info( s"Extracted ${fetched_records_native.count} records from table ${tableName} via the connector (no filter --> use the Java API)" );

                assert( (fetched_records_native.count == expected_num_records),
                        s"Fetched dataframe size (${fetched_records_native.count}) should be ${expected_num_records}" );
            }


            // Test the JDBC connector path
            // ----------------------------
            // Get the data out using the connecotr via the JDBC connector (when filtering IS necessary)
            val filter_expression = s"i < 1000";
            val fetched_records_jdbc = m_sparkSession.sqlContext.read
                                          .format( package_to_test )
                                          .options( egress_options ).load()
                                          .filter( filter_expression );
            logger.info( s"Extracted ${fetched_records_jdbc.count} records from table ${tableName} via the connector (with filter --> use the JDBC connector)" );

            assert( (fetched_records_jdbc.count == expected_num_records),
                    s"Fetched dataframe size (${fetched_records_jdbc.count}) should be ${expected_num_records}" );
        }  // end test #4 for KECO-1611



        /**
         * Test for egressing with offset and limit, where the offset is negative
         */
        test(s"""$package_description KECO-1611: Case 5 -- use offset and limit
             | for egressing from Kinetica with negative offset
             | """.stripMargin.replaceAll("\n", "") ) {

            logger.info( s"""$package_description KECO-1611: Case 5 -- use offset and limit
             | for egressing from Kinetica with negative offset
             | """.stripMargin.replaceAll("\n", "" ) );

            // This test fetches some data from the table, and ensures that
            // the offset and limit were honored

            // Create a table type with date, time, datetime columns
            val col_name = "i";
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( col_name, classOf[java.lang.Integer] );

            // Create the table (but clear any pre-existing ones)
            val initialTableName  = s"keco_1611_offset_limit_${package_description}";
            logger.debug( s"Table name '${initialTableName}'" );
            val tableName = createKineticaTableWithGivenColumns( initialTableName, columns, 0 );

            // Generate some test data
            // -----------------------
            val numRecords = 1000;
            logger.debug(s"Inserting $numRecords sequential integers");
            val random_options = immutable.Map[String,
                                               java.util.Map[String, java.lang.Double]](
                                    col_name ->
                                    immutable.Map[String,
                                                  java.lang.Double]("min" -> 1,
                                                                    "max" -> numRecords,
                                                                    "interval" -> 1).asJava ).asJava;
            m_gpudb.insertRecordsRandom( tableName, numRecords, random_options );

            // Check that the table size is correct
            val table_size = get_table_size( tableName );
            assert( (table_size == numRecords), s"Table size ($table_size) should be ${numRecords}" );

            // Test offset and limit for egress
            // --------------------------------
            val offset     = -100;
            val batch_size = 50;
            val expected_num_records = 0;

            // Get the appropriate egress options
            var egress_options = get_default_spark_connector_options();
            egress_options( "table.name"    ) = tableName;
            egress_options( "egress.offset" ) = s"$offset";
            egress_options( "egress.limit"  ) = s"$batch_size";

            // Test the Java API path
            // ----------------------
            try {
                // Get the data out using the connecotr via the Java API (when no filtering is necessary)
                val fetched_records_native = m_sparkSession.sqlContext.read.format( package_to_test )
                    .options( egress_options ).load();
                logger.info( s"Extracted ${fetched_records_native.count} records from table ${tableName} via the connector (no filter --> use the Java API)" );
            } catch {
                case e: java.lang.Exception => {
                    logger.debug( s"Got expected Java exception {}", e );
                }
                case e2: java.lang.RuntimeException => {
                    logger.debug( s"Got expected RUNTIME exception {}", e2 );
                }
            }

            // Test the JDBC connector path
            // ----------------------------
            try {
                // Get the data out using the connecotr via the JDBC connector (when filtering IS necessary)
                val filter_expression = s"i < 1000";
                val fetched_records_jdbc = m_sparkSession.sqlContext.read
                .format( package_to_test )
                .options( egress_options ).load()
                .filter( filter_expression );
            } catch {
                case e: java.lang.Exception => {
                    logger.debug( s"Got expected Java exception {}", e );
                }
                case e2: java.lang.RuntimeException => {
                    logger.debug( s"Got expected RUNTIME exception {}", e2 );
                }
            }

        }  // end test #5 for KECO-1611


        /**
         * Test for egressing with offset and limit, where the limit is negative
         */
        test(s"""$package_description KECO-1611: Case 6 -- use offset and limit
             | for egressing from Kinetica with negative limit
             | """.stripMargin.replaceAll("\n", "") ) {

            logger.info( s"""$package_description KECO-1611: Case 6 -- use offset and limit
             | for egressing from Kinetica with negative limit
             | """.stripMargin.replaceAll("\n", "" ) );

            // This test fetches some data from the table, and ensures that
            // the offset and limit were honored

            // Create a table type with date, time, datetime columns
            val col_name = "i";
            var columns : mutable.ListBuffer[Type.Column] = new mutable.ListBuffer[Type.Column]();
            columns += new Type.Column( col_name, classOf[java.lang.Integer] );

            // Create the table (but clear any pre-existing ones)
            val initialTableName  = s"keco_1611_offset_limit_${package_description}";
            logger.debug( s"Table name '${initialTableName}'" );
            val tableName = createKineticaTableWithGivenColumns( initialTableName, columns, 0 );

            // Generate some test data
            // -----------------------
            val numRecords = 1000;
            logger.debug(s"Inserting $numRecords sequential integers");
            val random_options = immutable.Map[String,
                                               java.util.Map[String, java.lang.Double]](
                                    col_name ->
                                    immutable.Map[String,
                                                  java.lang.Double]("min" -> 1,
                                                                    "max" -> numRecords,
                                                                    "interval" -> 1).asJava ).asJava;
            m_gpudb.insertRecordsRandom( tableName, numRecords, random_options );

            // Check that the table size is correct
            val table_size = get_table_size( tableName );
            assert( (table_size == numRecords), s"Table size ($table_size) should be ${numRecords}" );

            // Test offset and limit for egress
            // --------------------------------
            val offset     = 100;
            val batch_size = -50;
            val expected_num_records = 0;

            // Get the appropriate egress options
            var egress_options = get_default_spark_connector_options();
            egress_options( "table.name"    ) = tableName;
            egress_options( "egress.offset" ) = s"$offset";
            egress_options( "egress.limit"  ) = s"$batch_size";

            // Test the Java API path
            // ----------------------
            try {
                // Get the data out using the connecotr via the Java API (when no filtering is necessary)
                val fetched_records_native = m_sparkSession.sqlContext.read.format( package_to_test )
                    .options( egress_options ).load();
                logger.info( s"Extracted ${fetched_records_native.count} records from table ${tableName} via the connector (no filter --> use the Java API)" );
            } catch {
                case e: java.lang.Exception => {
                    logger.debug( s"Got expected Java exception {}", e );
                }
                case e2: java.lang.RuntimeException => {
                    logger.debug( s"Got expected RUNTIME exception {}", e2 );
                }
            }

            // Test the JDBC connector path
            // ----------------------------
            try {
                // Get the data out using the connecotr via the JDBC connector (when filtering IS necessary)
                val filter_expression = s"i < 1000";
                val fetched_records_jdbc = m_sparkSession.sqlContext.read
                .format( package_to_test )
                .options( egress_options ).load()
                .filter( filter_expression );
            } catch {
                case e: java.lang.Exception => {
                    logger.debug( s"Got expected Java exception {}", e );
                }
                case e2: java.lang.RuntimeException => {
                    logger.debug( s"Got expected RUNTIME exception {}", e2 );
                }
            }

        }  // end test #6 for KECO-1611


    }   // end tests for newFeatures



}   // end trait SparkConnectorNewFeatures



/**
 *  Test bug fixes using the DataSource v1 package.
 */
class TestNewFeatures_V1
    extends SparkConnectorTestBase
            with SparkConnectorNewFeatures {

    override val m_package_descr   = m_v1_package_descr;
    override val m_package_to_test = m_v1_package;

    // Run the tests
    testsFor( newFeatures( m_package_to_test, m_package_descr ) );

}  // TestNewFeatures_V1



/**
 *  Test bug fixes using the DataSource v2 package.
 */
class TestNewFeatures_V2
    extends SparkConnectorTestBase
            with SparkConnectorNewFeatures {

    override val m_package_descr   = m_v2_package_descr;
    override val m_package_to_test = m_v2_package;

    // Run the tests
    testsFor( newFeatures( m_package_to_test, m_package_descr ) );

}  // end TestNewFeatures_V2
