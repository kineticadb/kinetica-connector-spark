package com.kinetica.spark;

import com.gpudb.Type;
import com.gpudb.ColumnProperty;

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
     * Tests for using default and actual schema names when fully
     * qualified table name is provided during ingest into Kinetica.
     */
    def kineticaIngestion( package_to_test: String, package_description: String ) {

        test(s"""$package_description Kinetica Ingestion: Providing empty table.name
             | should result in an error""".stripMargin.replaceAll("\n", "") ) {
            // Get a dataframe
            val numRows = 10;
            val df = createDataFrameTwoIntNullableColumns( numRows );
            logger.debug(s"Created a dataframe with random data in it (${df.count} rows)");

            val dstTableName = "";
            logger.debug( s"Table name is empty" );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "true";
            options( "table.name"                 ) = dstTableName;

            // Write to the table
            logger.debug( s"Attempting to write to table ${dstTableName} via the connector" );

            // Providing empty table name would cause an exception.
            // We can expect an exception, but its wrapper is different for DataSource v1 and v2
            try {
                df.write.format( package_to_test ).options( options ).save();
            } catch {
                case e: java.lang.RuntimeException => {
                    assert(e.getCause.toString.contains("table name may not be empty"),
                        "Expected Kinetica to throw an error, because table.name is empty. ");
                    assert( package_to_test == m_v1_package,
                            "java.lang.RuntimeException exception is thrown for DataSource v1 only.");
                }
                case e: com.gpudb.GPUdbException   => {
                    assert(e.getMessage.toString.contains("table name may not be empty"),
                       "Expected Kinetica to throw an error, because table.name is empty. ");
                    assert( package_to_test == m_v2_package,
                            "com.gpudb.GPUdbException is thrown for DataSource v2 only.");
                }
            }
        } // end test #1 for Kinetica Ingestion


        test(s"""$package_description Kinetica Ingestion: Table name with
             | NO period and table.create flag set to TRUE extraction
             | from preexisting table should result in SUCCESS when table
             | is in the default schema""".stripMargin.replaceAll("\n", "") ) {
            // Get a dataframe
            val numRows = 10;
            val df = createDataFrameTwoIntNullableColumns( numRows );
            logger.debug(s"Created a dataframe with random data in it (${df.count} rows)");

            val initialTableName = "SparkConnectorTesting_keco_1679_table";
            logger.debug( s"Table name '${initialTableName}'" );

            var columns : ListBuffer[Type.Column] = new ListBuffer[Type.Column]();
            columns += new Type.Column( "x", classOf[java.lang.Integer], ColumnProperty.NULLABLE );
            columns += new Type.Column( "y", classOf[java.lang.Integer], ColumnProperty.NULLABLE );

            val dstTableName = createKineticaTableWithGivenColumns( initialTableName, columns, 0 );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "true";
            options( "table.name"                 ) = dstTableName;

            // Write to the table
            logger.debug( s"Writing to table ${dstTableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();
            logger.debug( s"After writing to table ${dstTableName} via the connector" );

            // Check that the table size is correct
            val table_size = get_table_size( dstTableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );

            // Check that the table has a schema name
            val schemaName = get_table_schema_name( dstTableName );
            assert ( schemaName.isDefined,
                     s"""Table '$dstTableName' should belong to default schema;
                  | but no schema found""".stripMargin.replaceAll("\n", "") );
        } // end test #2 for Kinetica Ingestion


        test(s"""$package_description Kinetica Ingestion: Table name with
             | NO period and table.create flag set to TRUE extraction
             | should result in a table in the default schema""".stripMargin.replaceAll("\n", "") ) {
            // Get a dataframe
            val numRows = 10;
            val df = createDataFrameTwoIntNullableColumns( numRows );
            logger.debug(s"Created a dataframe with random data in it (${df.count} rows)");

            val dstTableName = "SparkConnectorTesting_keco_1679";
            logger.debug( s"Table name '${dstTableName}'" );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( dstTableName );

            // Clear any existing table with this name
            clear_table( dstTableName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "true";
            options( "table.name"                 ) = dstTableName;

            // Write to the table
            logger.debug( s"Writing to table ${dstTableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();
            logger.debug( s"After writing to table ${dstTableName} via the connector" );

            // Check that the table size is correct
            val table_size = get_table_size( dstTableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );

            // Check that the table has a schema name
            val schemaName = get_table_schema_name( dstTableName );
            assert ( schemaName.isDefined,
                     s"""Table '$dstTableName' should belong to default schema;
                  | but no schema found""".stripMargin.replaceAll("\n", "") );
        } // end test #3 for Kinetica Ingestion


        test(s"""$package_description Kinetica Ingestion: Table name with
             | NO period and table.create flag set to FALSE extraction
             | should fail""".stripMargin.replaceAll("\n", "") ) {
            // Get a dataframe
            val numRows = 10;
            val df = createDataFrameTwoIntNullableColumns( numRows );
            logger.debug(s"Created a dataframe with random data in it (${df.count} rows)");

            val dstTableName = "SparkConnectorTesting_keco_1679";
            logger.debug( s"Table name '${dstTableName}'" );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( dstTableName );

            // Clear any existing table with this name
            clear_table( dstTableName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "false";
            options( "table.name"                 ) = dstTableName;

            // Write to the table
            logger.debug( s"Attempting to write to table ${dstTableName} via the connector" );

            // Expect the write to fail because table is not created.
            // We can expect an exception, but its wrapper is different for DataSource v1 and v2
            try {
                df.write.format( package_to_test ).options( options ).save();
            } catch {
                case e: java.lang.RuntimeException => {
                    assert( e.getCause.toString.contains(s"Table '${dstTableName}' does not exist"),
                            "Expected Kinetica to throw an error, because table does not exist." );
                    assert( package_to_test == m_v1_package,
                            "java.lang.RuntimeException exception is thrown for DataSource v1 only.");
                }
                case e: org.apache.spark.SparkException   => {
                    assert( e.getCause.toString.contains("No matching table or schema was found"),
                            "Expected Kinetica to throw an error, because table does not exist." );
                    assert( package_to_test == m_v2_package,
                            "org.apache.spark.SparkException is thrown for DataSource v2 only.");
                }
            }
        } // end test #4 for Kinetica Ingestion


        test(s"""$package_description Kinetica Ingestion: Table name WITH
             | period and table.create flag set to FALSE extraction
             | should fail""".stripMargin.replaceAll("\n", "") ) {
            // Get a dataframe
            val numRows = 10;
            val df = createDataFrameTwoIntNullableColumns( numRows );
            logger.debug(s"Created a dataframe with random data in it (${df.count} rows)");

            // Table name needs to have periods
            val dstTableName = "SparkConnectorTestSuite.keco_1679_failure";
            logger.debug( s"Table name '${dstTableName}'" );

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( dstTableName );

            // Clear any existing table with this name
            clear_table( dstTableName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "false";
            options( "table.name"                 ) = dstTableName;

            // Write to the table
            logger.debug( s"Attempting to write to table ${dstTableName} via the connector" );

            // Expect the write to fail because table is not created.
            // We can expect an exception, but its wrapper is different for DataSource v1 and v2
            try {
                df.write.format( package_to_test ).options( options ).save();
            } catch {
                case e: java.lang.RuntimeException => {
                    assert( e.getCause.toString.contains(s"Table '${dstTableName}' does not exist"),
                            "Expected Kinetica to throw an error, because table does not exist." );
                    assert( package_to_test == m_v1_package,
                            "java.lang.RuntimeException exception is thrown for DataSource v1 only.")
                }
                case e: org.apache.spark.SparkException   => {
                    assert( e.getCause.toString.contains("No matching table or schema was found"),
                            "Expected Kinetica to throw an error, because table does not exist." );
                    assert( package_to_test == m_v2_package,
                            "org.apache.spark.SparkException is thrown for DataSource v2 only.")
                }
            }
        } // end test #5 for Kinetica Ingestion


        test(s"""$package_description Kinetica Ingestion: Table name WITH period
             | and create.table flag set to TRUE should be treated as a name in
             | <SCHEMA>.<TABLE> notation and should result in a table <TABLE>
             | created IN the schema <SCHEMA>""".stripMargin.replaceAll("\n", "") ) {
            // Get a dataframe
            val numRows = 10;
            val df = createDataFrameTwoIntNullableColumns( numRows );
            logger.debug(s"Created a dataframe with random data in it (${df.count} rows)");

            // Table name needs to have periods
            val dstTableName = "SparkConnectorTestSuite.keco_1679";
            logger.debug( s"Table name '${dstTableName}'" );
            val dstSchemaName = "SparkConnectorTestSuite";

            // Need to mark the table name for post-test clean-up
            mark_table_for_deletion_at_test_end( dstTableName );

            val schema_exists = does_schema_exist( dstSchemaName );
            if (!schema_exists) {
                mark_schema_for_deletion_at_test_end( dstSchemaName );
            }

            // Clear any existing table with this name
            clear_table( dstTableName );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "true";
            options( "table.name"                 ) = dstTableName;

            // Write to the table
            logger.debug( s"Writing to table ${dstTableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            val table_size = get_table_size( dstTableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );

            // Check that the table belongs to the given schema
            val schemaName = get_table_schema_name( dstTableName );
            assert ( schemaName == Option.apply(dstSchemaName),
                     s"""Table '$dstTableName' expected to be in schema '$dstSchemaName';
                     | found to be in '${schemaName.getOrElse("None")}'
                     |""".stripMargin.replaceAll("\n", "") );
        } // end test #6 for Kinetica Ingestion


        test(s"""$package_description Kinetica Ingestion: Table name WITH period
             | and create.table flag set to TRUE and preexisting <SCHEMA>.<TABLE>
             | with matching type should result in success""".stripMargin.replaceAll("\n", "") ) {
            // Get a dataframe
            val numRows = 10;
            val df = createDataFrameTwoIntNullableColumns( numRows );
            logger.debug(s"Created a dataframe with random data in it (${df.count} rows)");

            // Table name needs to have periods
            val initialTableName = "SparkConnectorTestSuite.keco_1679";
            logger.debug( s"Table name '${initialTableName}'" );
            val dstSchemaName = "SparkConnectorTestSuite";

            var columns : ListBuffer[Type.Column] = new ListBuffer[Type.Column]();
            columns += new Type.Column( "x", classOf[java.lang.Integer], ColumnProperty.NULLABLE );
            columns += new Type.Column( "y", classOf[java.lang.Integer], ColumnProperty.NULLABLE );

            val dstTableName = createKineticaTableWithGivenColumns( initialTableName, columns, 0 );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "false";
            options( "table.name"                 ) = dstTableName;
            options( "table.map_columns_by_name"  ) = "true";

            // Write to the table
            logger.debug( s"Writing to table ${dstTableName} via the connector" );
            df.write.format( package_to_test ).options( options ).save();

            // Check that the table size is correct
            val table_size = get_table_size( dstTableName );
            assert( (table_size == numRows), s"Table size ($table_size) should be $numRows" );

            // Check that the table belongs to the given schema
            val schemaName = get_table_schema_name( dstTableName );
            assert ( schemaName == Option.apply(dstSchemaName),
                     s"""Table '$dstTableName' expected to be in schema '$dstSchemaName';
                     | found to be in '${schemaName.getOrElse("None")}'
                     |""".stripMargin.replaceAll("\n", "") );
        } // end test #7 for Kinetica Ingestion


        test(s"""$package_description Kinetica Ingestion: Table name with
             | MORE than one period when create.table flag is set to TRUE
             | should fail to ingest""".stripMargin.replaceAll("\n", "") ) {
            // Get a dataframe
            val numRows = 10;
            val df = createDataFrameTwoIntNullableColumns( numRows );
            logger.debug(s"Created a dataframe with random data in it (${df.count} rows)");

            // Table name needs to have periods
            val fullTableName = "SparkConnector.TestSuite.table.1679";
            val expectedSchemaName = "SparkConnector";

            logger.debug( s"Table name, as given to the spark connector: '${fullTableName}'" );
            logger.debug( s"Schema name '${expectedSchemaName}'" );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.create"               ) = "true";
            options( "table.name"                 ) = fullTableName;

            // Write to the table
            logger.debug( s"Attempting to write to table ${fullTableName} via the connector" );

            // Expect the write to fail because table is not created.
            try {
                df.write.format( package_to_test ).options( options ).save();
            } catch {
                case e: java.lang.Exception => {
                    assert( e.getMessage.toString.contains("cannot contain more than one dot"),
                            "Expected Kinetica to throw an error, because table.name is invalid." );
                }
            }
        } // end test #8 for Kinetica Ingestion
    }   // end tests for Kinetica Ingestion



    /**
     * Tests for using default and actual schema names when fully
     * qualified table name is provided during Kinetica read.
     */
    def kineticaEgress( package_to_test: String, package_description: String ) {


        test(s"""$package_description Kinetica Egress: Providing an empty table
             | name results in a failure""".stripMargin.replaceAll("\n", "") ) {
            // Use an invalid table name to attempt a read from Kinetica
            val tableName = "";
            logger.debug( s"Table name is empty" );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = tableName;

            // Read from the table
            logger.debug( s"Attempting to read from table '${tableName}' via the connector" );

            // We can expect an exception, but its wrapper is different for DataSource v1 and v2
            try {
                val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
                // // Need to comment this out until KECO-1241 is fixed
                // logger.debug( s"Got dataframe of size: ${df.count}" );

                df.show(); // Until KECO-1241 is fixed, use this
            } catch {
                case e: java.lang.RuntimeException => {
                    assert( e.getMessage.toString.contains("table name may not be empty"),
                            "Expected Kinetica to throw an error, because table.name is empty." );
                    assert( package_to_test == m_v1_package,
                            "java.lang.RuntimeException exception is thrown for DataSource v1 only.");
                }
                case e: com.gpudb.GPUdbException   => {
                    assert( e.getMessage.toString.contains("table name may not be empty"),
                            "Expected Kinetica to throw an error, because table.name is empty." );
                    assert( package_to_test == m_v2_package,
                            "com.gpudb.GPUdbException is thrown for DataSource v2 only.");
                }
            }
        } // end test #1 for Kinetica Egress


        test(s"""$package_description Kinetica Egress: Reading from a table
             | name with NO period should not work when the table IS
             | in schema different from default
             |""".stripMargin.replaceAll("\n", "") ) {
            // Create a Kinetica table (with NO period in the name)
            val numRows = 10;
            val initialTableName = "keco_1679_table";
            val collectionName = "SparkConnectorTestSuite_1679_collection";
            val fullName = s"${collectionName}.${initialTableName}"
            logger.debug( s"Table name '${fullName}'" );

            var columns : ListBuffer[Type.Column] = new ListBuffer[Type.Column]();
            columns += new Type.Column( "x", classOf[java.lang.Integer], ColumnProperty.NULLABLE );
            val tableName = createKineticaTableWithGivenColumns( fullName, columns, numRows );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = tableName;

            // Read from the table
            logger.debug( s"Reading from table ${tableName} via the connector" );

            // We can expect an exception
            //assertThrows[ java.lang.RuntimeException ]
            try {
                val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
                // // Need to comment this out until KECO-1241 is fixed
                // logger.debug( s"Got dataframe of size: ${df.count}" );

                df.show(); // Until KECO-1241 is fixed, use this

                // // Need to comment this out until KECO-1241 is fixed
                // // Check that the dataframe size is correct
                // assert( (df.count == numRows), s"DataFrame size (${df.count}) should be $numRows" );
            } catch {
                case e: java.lang.RuntimeException => {
                    if ( package_to_test == m_v2_package ) {
                        assert( e.getMessage.toString.contains(s"Table '${tableName}' does not exist!"),
                                "Expected Kinetica to throw an error, because table does not exist.");
                    }
                }
            }
        } // end test #2 for Kinetica Egress


        test(s"""$package_description Kinetica Egress: Reading from a table
             | name with NO period should work fine when the table is in
             | the default schema
             |""".stripMargin.replaceAll("\n", "") ) {
            // Create a Kinetica table (with NO period in the name)
            val numRows = 10;
            val initialTableName = "SparkConnectorTestSuite_1679_table";
            logger.debug( s"Table name '${initialTableName}'" );
            var columns : ListBuffer[Type.Column] = new ListBuffer[Type.Column]();
            columns += new Type.Column( "x", classOf[java.lang.Integer], ColumnProperty.NULLABLE );
            val tableName = createKineticaTableWithGivenColumns( initialTableName, columns, numRows );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = tableName;

            // Read from the table
            logger.debug( s"Reading from table ${tableName} via the connector" );
            val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
            // // Need to comment this out until KECO-1241 is fixed
            // logger.debug( s"Got dataframe of size: ${df.count}" );

            df.show(); // Until KECO-1241 is fixed, use this

            // // Need to comment this out until KECO-1241 is fixed
            // // Check that the dataframe size is correct
            // assert( (df.count == numRows), s"DataFrame size (${df.count}) should be $numRows" );
        } // end test #3 for Kinetica Egress


        test(s"""$package_description Kinetica Egress: Reading from a table
             | name WITH period in it should not work
             | when the table is NOT in the given schema
             |""".stripMargin.replaceAll("\n", "") ) {
            // Create a Kinetica table without a period in the name,
            // then attempt to read data giving a name with period
            val numRows = 10;
            val initialTableName = "keco_1679_table";
            val collectionName = "SparkConnectorTestSuite_1679_collection";
            val fullTableName = s"${collectionName}.${initialTableName}"
            logger.debug( s"Table name '${fullTableName}'" );
            var columns : ListBuffer[Type.Column] = new ListBuffer[Type.Column]();
            columns += new Type.Column( "x", classOf[java.lang.Integer], ColumnProperty.NULLABLE );
            val fullName = createKineticaTableWithGivenColumns( fullTableName, columns, numRows );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = fullName;

            // Read from the table
            logger.debug( s"Reading from table ${fullName} via the connector" );

            // We can expect an exception
            try {
                val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
                // // Need to comment this out until KECO-1241 is fixed
                // logger.debug( s"Got dataframe of size: ${df.count}" );

                df.show(); // Until KECO-1241 is fixed, use this

                // // Need to comment this out until KECO-1241 is fixed
                // // Check that the dataframe size is correct
                // assert( (df.count == numRows), s"DataFrame size (${df.count}) should be $numRows" );
            } catch {
                case e: java.lang.RuntimeException => {
                    if ( package_to_test == m_v2_package ) {
                        assert( e.getMessage.toString.contains(s"Table '${fullName}' does not exist!"),
                                "Expected Kinetica to throw an error, because table does not exist.");
                    }
                }
            }
        }// end test #4 for Kinetica Egress


        test(s"""$package_description Kinetica Egress: Reading from a table
             | name WITH one period should work fine when the table
             | IS in the given schema""".stripMargin.replaceAll("\n", "") ) {
            // Create a Kinetica table with a period in the name,
            // then attempt to read data giving the same name
            val numRows = 10;
            val initialTableName = "SparkConnectorTestSuite_coll.keco_1679_table";
            logger.debug( s"Table name '${initialTableName}'" );
            var columns : ListBuffer[Type.Column] = new ListBuffer[Type.Column]();
            columns += new Type.Column( "x", classOf[java.lang.Integer], ColumnProperty.NULLABLE );
            val tableName = createKineticaTableWithGivenColumns( initialTableName, columns, numRows );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = tableName;

            // Read from the table
            logger.debug( s"Reading from table ${tableName} via the connector" );
            val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
            // // Need to comment this out until KECO-1241 is fixed
            // logger.debug( s"Got dataframe of size: ${df.count}" );

            df.show(); // Until KECO-1241 is fixed, use this

            // // Need to comment this out until KECO-1241 is fixed
            // // Check that the dataframe size is correct
            // assert( (df.count == numRows), s"DataFrame size (${df.count}) should be $numRows" );
        } // end test #5 for Kinetica Egress


        test(s"""$package_description Kinetica Egress: Reading from a table
             | name with MORE than one period should fail""".stripMargin.replaceAll("\n", "") ) {
            // Configure session to use table name with multiple periods, attempt to read data
            val tableName = "SparkConnector.TestSuite.table.1679";
            val expectedSchemaName = "SparkConnector";

            logger.debug( s"Table name, as given to the spark connector: '${tableName}'" );
            logger.debug( s"Schema name '${expectedSchemaName}'" );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = tableName;

            // Write to the table
            logger.debug( s"Attempting to read from a non-existing table ${tableName} via the connector" );

            // Expect the write to fail because table is not created.
            try {
               val df = m_sparkSession.read.format( package_to_test ).options( options ).load();
            } catch {
                case e: java.lang.Exception => {
                    assert( e.getMessage.toString.contains("cannot contain more than one dot"),
                            "Expected Kinetica to throw an error, because table.name is invalid." );
                }
            }
        } // end test #6 for Kinetica Egress


        ignore(s"""$package_description Kinetica Egress: The count() method on dataframe
             | created by reading from a Kinetica table should work fine.
             |""".stripMargin.replaceAll("\n", "") ) {
            // Note: When KECO-1241 is fixed, need to go to the above egress tests and fix
            //       them (to use df.count).

            // Create a Kinetica table (with NO period in the name)
            val numRows = 10;
            val initialTableName = "keco_1241_table";
            var columns : ListBuffer[Type.Column] = new ListBuffer[Type.Column]();
            columns += new Type.Column( "x", classOf[java.lang.Integer], ColumnProperty.NULLABLE );
            val tableName = createKineticaTableWithGivenColumns( initialTableName, columns, numRows );

            // Get the appropriate options
            var options = get_default_spark_connector_options();
            options( "table.name"                 ) = tableName;

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
