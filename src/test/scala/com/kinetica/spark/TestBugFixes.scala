package com.kinetica.spark;

import org.scalatest.FunSuite;
import org.apache.spark.sql.SparkSession;
import com.typesafe.scalalogging.LazyLogging;
import scala.collection.JavaConverters._;
import scala.collection.mutable.ListBuffer;


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
     * Tests for schema/collection name extraction from table name 
     * when ingesting into Kinetica.
     */
    def bugFixes( package_to_test: String, package_description: String ) {

        test(s"""$package_description KECO-1396: Table data egressed from a long
             | long (timestamp) column should be able to re-ingested in the same
             | column without issues""".stripMargin.replaceAll("\n", "") ) {

            // Create the table (but clear any pre-existing ones)
            val tableName = "keco_1396";
            clear_table( tableName );
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableOneLongTimestampNullableColumn( tableName, None, 0 );

            // // Need to mark the table name for post-test clean-up
            // mark_table_for_deletion_at_test_end( tableName );


            // Read the data from the test file
            val numRows = 2;
            val df = createDataFrameOneStringTimestampNullableColumn( numRows );
            logger.debug(s"Created a dataframe with random timestamp string in it (${df.count} rows)");

            
            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "false";
            options( "table.name"                 ) = tableName;

            // Write to the table
            logger.debug( s"Writing to table ${tableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            val table_size = get_table_size( tableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );
        }


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
