package com.kinetica.spark;

import org.scalatest.FunSuite;
import org.apache.spark.sql.SparkSession;
import com.typesafe.scalalogging.LazyLogging;
import scala.collection.JavaConverters._;
import scala.collection.mutable.ListBuffer;


/**
 * This trait contains the test cases for table name parsing.  There are
 * tests for both ingestion and egress.  In order to run these tests,
 * a class is needed that mixes in this trait and uses the `testsFor()`
 * method which invokes the given behavior function.
 *
 */
trait TableNameCollectionExtractionBehavior
    extends SparkConnectorTestFixture { this: FunSuite =>

    /**
     * Tests for schema/collection name extraction from table name 
     * when ingesting into Kinetica.
     */
    def kineticaIngestion( package_to_test: String, package_description: String ) {

        test(s"""$package_description Kinetica Ingestion: Table name with NO period
             | and NO collection name extraction should result in a table
             | NOT in a collection""".stripMargin.replaceAll("\n", "") ) {
            // Get a dataframe
            val numRows = 10;
            val df = createDataFrameTwoIntNullableColumns( numRows );
            logger.debug(s"Created a dataframe with random data in it (${df.count} rows)");

            // Table name needs to have periods
            val dstTableName = "keco_1232";
            logger.debug( s"Table name '${dstTableName}'" );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( dstTableName );

            // Clear any existing table with this name
            clear_table( dstTableName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "true";
            options( "table.name"                 ) = dstTableName;
            options( "table.name_contains_schema" ) = "false";

            // Write to the table
            logger.debug( s"Writing to table ${dstTableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            val table_size = get_table_size( dstTableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );

            // Check that the table did not get put in a collection
            val collectionName = get_table_collection_name( dstTableName );
            assert ( !collectionName.isDefined,
                     s"""Table '$dstTableName' should not be in any collection;
                  | found to be in '${collectionName.getOrElse("None")}'
                  |""".stripMargin.replaceAll("\n", "") );
        }


        test(s"""$package_description Kinetica Ingestion: Table name with NO period
             | and WITH collection name extraction should result in a table
             | NOT in a collection""".stripMargin.replaceAll("\n", "") ) {
            // Get a dataframe
            val numRows = 10;
            val df = createDataFrameTwoIntNullableColumns( numRows );
            logger.debug(s"Created a dataframe with random data in it (${df.count} rows)");

            // Table name needs to have periods
            val dstTableName = "keco_1232";
            logger.debug( s"Table name '${dstTableName}'" );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( dstTableName );

            // Clear any existing table with this name
            clear_table( dstTableName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "true";
            options( "table.name"                 ) = dstTableName;
            options( "table.name_contains_schema" ) = "true";

            // Write to the table
            logger.debug( s"Writing to table ${dstTableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            val table_size = get_table_size( dstTableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );

            // Check that the table did not get put in a collection
            val collectionName = get_table_collection_name( dstTableName );
            assert ( !collectionName.isDefined,
                     s"""Table '$dstTableName' should not be in any collection;
                  | found to be in '${collectionName.getOrElse("None")}'
                  |""".stripMargin.replaceAll("\n", "") );
        }


        test(s"""$package_description Kinetica Ingestion: Table name with WITH period
             | and NO collection name extraction should result in a table
             | NOT in a collection""".stripMargin.replaceAll("\n", "") ) {
            // Get a dataframe
            val numRows = 10;
            val df = createDataFrameTwoIntNullableColumns( numRows );
            logger.debug(s"Created a dataframe with random data in it (${df.count} rows)");

            // Table name needs to have periods
            val dstTableName = "keco.1232";
            logger.debug( s"Table name '${dstTableName}'" );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( dstTableName );

            // Clear any existing table with this name
            clear_table( dstTableName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "true";
            options( "table.name"                 ) = dstTableName;
            options( "table.name_contains_schema" ) = "false";

            // Write to the table
            logger.debug( s"Writing to table ${dstTableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            val table_size = get_table_size( dstTableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );

            // Check that the table did not get put in a collection
            val collectionName = get_table_collection_name( dstTableName );
            assert ( !collectionName.isDefined,
                     s"""Table '$dstTableName' should not be in any collection;
                  | found to be in '${collectionName.getOrElse("None")}'
                  |""".stripMargin.replaceAll("\n", "") );
        }


        test(s"""$package_description Kinetica Ingestion: Table name with WITH period
             | and WITH collection name extraction should result in a table
             | IN a collection""".stripMargin.replaceAll("\n", "") ) {
            // Get a dataframe
            val numRows = 10;
            val df = createDataFrameTwoIntNullableColumns( numRows );
            logger.debug(s"Created a dataframe with random data in it (${df.count} rows)");

            // Table name needs to have periods
            val fullTableName = "keco.table.1232";
            val expectedCollectionName = "keco";
            val expectedTableName = "table.1232";
            logger.debug( s"Table name, as given to the spark connector: '${fullTableName}'" );
            logger.debug( s"Table name '${expectedTableName}'" );
            logger.debug( s"Colelction name '${expectedCollectionName}'" );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( expectedTableName );

            // Clear any existing table with this name
            clear_table( fullTableName );
            clear_table( expectedTableName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "true";
            options( "table.name"                 ) = fullTableName;
            options( "table.name_contains_schema" ) = "true";

            // Write to the table
            logger.debug( s"Writing to table ${fullTableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            val table_size = get_table_size( expectedTableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );

            // Check that the table did not get put in a collection
            val collectionName = get_table_collection_name( expectedTableName );
            assert ( collectionName.isDefined,
                     s"""Table '$expectedTableName' should be in the collection
                  | '$expectedCollectionName'; found to be in 
                  | '${collectionName.getOrElse("None")}'
                  |""".stripMargin.replaceAll("\n", "") );
        }
    }   // end tests for kineticaIngestion



    /**
     * Tests for schema/collection name extraction from table name 
     * when reading from Kinetica.
     */
    def kineticaEgress( package_to_test: String, package_description: String ) {

        
        test(s"""$package_description Kinetica Egress: Reading from a table
             | name with NO period and NO collection name extraction should work
             | fine when the table is NOT in a collection
             |""".stripMargin.replaceAll("\n", "") ) {
            // Create a Kinetica table (with NO period in the name) NOT in a collection
            val numRows = 10;
            val tableName = "keco_1232_table";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableTwoIntNullableColumns( tableName, None, numRows );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( tableName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = tableName;
            options( "table.name_contains_schema" ) = "false";

            // Read from the table
            logger.debug( s"Reading from table ${tableName} via the connector" );
            val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
            // // Need to comment this out until KECO-1241 is fixed
            // logger.debug( s"Got dataframe of size: ${df.count}" );

            df.show(); // Until KECO-1241 is fixed, use this

            // // Need to comment this out until KECO-1241 is fixed
            // // Check that the dataframe size is correct
            // assert( (df.count == numRows), s"DataFrame size (${df.count}) should be $numRows" );
        }


        test(s"""$package_description Kinetica Egress: Reading from a table
             | name with NO period and NO collection name extraction should work
             | fine when the table IS in a collection
             |""".stripMargin.replaceAll("\n", "") ) {
            // Create a Kinetica table (with NO period in the name) NOT in a collection
            val numRows = 10;
            val tableName = "keco_1232_table";
            val collectionName = "keco_1232_collection";
            logger.debug( s"Table name '${tableName}'" );
            logger.debug( s"Collection name '${collectionName}'" );
            createKineticaTableTwoIntNullableColumns( tableName,
                                                      Option.apply( collectionName ),
                                                      numRows );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( tableName );
            mark_table_for_deletion_at_test_end( collectionName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = tableName;
            options( "table.name_contains_schema" ) = "false";

            // Read from the table
            logger.debug( s"Reading from table ${tableName} via the connector" );
            val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
            // // Need to comment this out until KECO-1241 is fixed
            // logger.debug( s"Got dataframe of size: ${df.count}" );

            df.show(); // Until KECO-1241 is fixed, use this

            // // Need to comment this out until KECO-1241 is fixed
            // // Check that the dataframe size is correct
            // assert( (df.count == numRows), s"DataFrame size (${df.count}) should be $numRows" );
        }


        test(s"""$package_description Kinetica Egress: Reading from a table
             | name with NO period and WITH collection name extraction should work
             | fine when the table is NOT in a collection
             |""".stripMargin.replaceAll("\n", "") ) {
            // Create a Kinetica table (with NO period in the name) NOT in a collection
            val numRows = 10;
            val tableName = "keco_1232_table";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableTwoIntNullableColumns( tableName, None, numRows );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( tableName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = tableName;
            options( "table.name_contains_schema" ) = "true";

            // Read from the table
            logger.debug( s"Reading from table ${tableName} via the connector" );
            val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
            // // Need to comment this out until KECO-1241 is fixed
            // logger.debug( s"Got dataframe of size: ${df.count}" );

            df.show(); // Until KECO-1241 is fixed, use this

            // // Need to comment this out until KECO-1241 is fixed
            // // Check that the dataframe size is correct
            // assert( (df.count == numRows), s"DataFrame size (${df.count}) should be $numRows" );
        }


        test(s"""$package_description Kinetica Egress: Reading from a table
             | name with NO period and WITH collection name extraction should work
             | fine when the table IS in a collection
             |""".stripMargin.replaceAll("\n", "") ) {
            // Create a Kinetica table (with NO period in the name) NOT in a collection
            val numRows = 10;
            val tableName = "keco_1232_table";
            val collectionName = "keco_1232_collection";
            logger.debug( s"Table name '${tableName}'" );
            logger.debug( s"Collection name '${collectionName}'" );
            createKineticaTableTwoIntNullableColumns( tableName,
                                                      Option.apply( collectionName ),
                                                      numRows );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( tableName );
            mark_table_for_deletion_at_test_end( collectionName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = tableName;
            options( "table.name_contains_schema" ) = "true";

            // Read from the table
            logger.debug( s"Reading from table ${tableName} via the connector" );
            val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
            // // Need to comment this out until KECO-1241 is fixed
            // logger.debug( s"Got dataframe of size: ${df.count}" );

            df.show(); // Until KECO-1241 is fixed, use this

            // // Need to comment this out until KECO-1241 is fixed
            // // Check that the dataframe size is correct
            // assert( (df.count == numRows), s"DataFrame size (${df.count}) should be $numRows" );
        }


        test(s"""$package_description Kinetica Egress: Reading from a table
             | name with WITH period and NO collection name extraction should work
             | fine when the table is NOT in a collection
             |""".stripMargin.replaceAll("\n", "") ) {
            // Create a Kinetica table (with NO period in the name) NOT in a collection
            val numRows = 10;
            val tableName = "keco.1232_table";
            logger.debug( s"Table name '${tableName}'" );
            createKineticaTableTwoIntNullableColumns( tableName, None, numRows );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( tableName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = tableName;
            options( "table.name_contains_schema" ) = "false";

            // Read from the table
            logger.debug( s"Reading from table ${tableName} via the connector" );
            val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
            // // Need to comment this out until KECO-1241 is fixed
            // logger.debug( s"Got dataframe of size: ${df.count}" );

            df.show(); // Until KECO-1241 is fixed, use this

            // // Need to comment this out until KECO-1241 is fixed
            // // Check that the dataframe size is correct
            // assert( (df.count == numRows), s"DataFrame size (${df.count}) should be $numRows" );
        }


        test(s"""$package_description Kinetica Egress: Reading from a table
             | name with WITH period and NO collection name extraction should work
             | fine when the table IS in a collection
             |""".stripMargin.replaceAll("\n", "") ) {
            // Create a Kinetica table (with NO period in the name) NOT in a collection
            val numRows = 10;
            val tableName = "keco.1232_table";
            val collectionName = "keco_1232_collection";
            logger.debug( s"Table name '${tableName}'" );
            logger.debug( s"Collection name '${collectionName}'" );
            createKineticaTableTwoIntNullableColumns( tableName,
                                                      Option.apply( collectionName ),
                                                      numRows );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( tableName );
            mark_table_for_deletion_at_test_end( collectionName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = tableName;
            options( "table.name_contains_schema" ) = "false";

            // Read from the table
            logger.debug( s"Reading from table ${tableName} via the connector" );
            val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
            // // Need to comment this out until KECO-1241 is fixed
            // logger.debug( s"Got dataframe of size: ${df.count}" );

            df.show(); // Until KECO-1241 is fixed, use this

            // // Need to comment this out until KECO-1241 is fixed
            // // Check that the dataframe size is correct
            // assert( (df.count == numRows), s"DataFrame size (${df.count}) should be $numRows" );
        }


        test(s"""$package_description Kinetica Egress: Reading from a table
             | name with WITH period and WITH collection name extraction should work
             | fine when the table IS in the given  collection
             |""".stripMargin.replaceAll("\n", "") ) {
            // Create a Kinetica table (with NO period in the name) NOT in a collection
            val numRows = 10;
            val tableName = "keco.1232_table";
            val collectionName = "keco_1232_collection";
            val tableNameWithCollection = s"$collectionName.$tableName";
            logger.debug( s"Table name '${tableName}'" );
            logger.debug( s"Collection name '${collectionName}'" );
            createKineticaTableTwoIntNullableColumns( tableName,
                                                      Option.apply( collectionName ),
                                                      numRows );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( tableName );
            mark_table_for_deletion_at_test_end( collectionName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            // The table name given to the connector contains the collection name
            options( "table.name"                 ) = tableNameWithCollection;
            options( "table.name_contains_schema" ) = "true";

            // Read from the table
            logger.debug( s"Reading from table ${tableName} via the connector" );
            val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
            // // Need to comment this out until KECO-1241 is fixed
            // logger.debug( s"Got dataframe of size: ${df.count}" );

            df.show(); // Until KECO-1241 is fixed, use this

            // // Need to comment this out until KECO-1241 is fixed
            // // Check that the dataframe size is correct
            // assert( (df.count == numRows), s"DataFrame size (${df.count}) should be $numRows" );
        }


        ignore(s"""$package_description Kinetica Egress: The count() method on dataframe
             | created by reading from a Kinetica table should work fine.
             |""".stripMargin.replaceAll("\n", "") ) {
            // Note: When KECO-1241 is fixed, need to go to the above egress tests and fix
            //       them (to use df.count).
            
            // Create a Kinetica table (with NO period in the name) NOT in a collection
            val numRows = 10;
            val tableName = "keco_1241_table";
            createKineticaTableTwoIntNullableColumns( tableName, None, numRows );

            // // Need to mark the table name for post-test clean-up
            // mark_table_for_deletion_at_test_end( tableName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = tableName;
            options( "table.name_contains_schema" ) = "false";

            // Read from the table
            logger.debug( s"Reading from table ${tableName} via the connector" );
            val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
            logger.debug( s"Got dataframe of size: ${df.count}" );

            // Check that the dataframe size is correct
            assert( (df.count == numRows), s"DataFrame size (${df.count}) should be $numRows" );
        }
    }  // end tests for kineticaEgress


}   // end trait TableNameCollectionExtractionBehavior




/**
 *  Test collection/schema name extraction from the given table name
 *  using the DataSource v1 package.
 */
class TestTableNameCollectionExtraction_V1
    extends SparkConnectorTestBase
            with TableNameCollectionExtractionBehavior {

    override val m_package_descr   = m_v1_package_descr;
    override val m_package_to_test = m_v1_package;

    
    // Run the tests when a table is created from a spark DataFrame
    testsFor( kineticaIngestion( m_package_to_test, m_package_descr ) );

    // Run the tests when a table is created from a spark DataFrame
    testsFor( kineticaEgress( m_package_to_test, m_package_descr ) );
    
}  // TestTableNameCollectionExtraction_V1



/**
 *  Test collection/schema name extraction from the given table name
 *  using the DataSource v2 package.
 */
class TestTableNameCollectionExtraction_V2
    extends SparkConnectorTestBase
            with TableNameCollectionExtractionBehavior {

    override val m_package_descr   = m_v2_package_descr;
    override val m_package_to_test = m_v2_package;

    // Run the tests when a table is created from a spark DataFrame
    testsFor( kineticaIngestion( m_package_to_test, m_package_descr ) );
    
    // Run the tests when a table is created from a spark DataFrame
    testsFor( kineticaEgress( m_package_to_test, m_package_descr ) );

}  // end TestTableNameCollectionExtraction_V2
