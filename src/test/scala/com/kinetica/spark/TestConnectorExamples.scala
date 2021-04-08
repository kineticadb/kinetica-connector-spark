package com.kinetica.spark;

import java.io._;
import com.gpudb.Type;
import com.gpudb.ColumnProperty;

import org.scalatest.FunSuite;
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
import scala.collection.mutable.ListBuffer;

/**
 * This trait contains the test cases for Kinetica Spark Connector
 * examples published with Kinetica documentation. In order to run these tests,
 * a class is needed that mixes in this trait and uses the `testsFor()`
 * method which invokes the given behavior function.
 *
 */
trait SparkConnectorExamplesBehavior
    extends SparkConnectorTestFixture { this: FunSuite =>

    /**
     * Tests for various Kinetica Spark Connector examples.
     */
    def kineticaExamples( package_to_test: String, package_description: String ) {

        /**
         * Test for dry run to never attempt establishing Kinetica connection.
         */
        test(s"""$package_description KECO-1826: Dry run should not attempt
             | to establish a Kinetica connection""".stripMargin.replaceAll("\n", "") ) {

            // Create a data set matching KECO-1826 csv file values
            val data = Seq(
                Row( 1, 2008, 1, 3, 4, 1343, 1325, 1451, 1435, "WN", 588, "N240WN", 68, 70, 55, 16, 18, "HOU", "LIT", 393, 4, 9, 0, "", 0, 16, 0, 0, 0, 0),
				Row( 2, 2008, 1, 3, 4, 1125, 1120, 1247, 1245, "WN", 1343, "N523SW", 82, 85, 71, 2, 5, "HOU", "MAF", 441, 3, 8, 0, "",0, null, null, null, null, null),
				Row( 3, 2008, 1, 3, 4, 2009, 2015, 2136, 2140, "WN", 3841, "N280WN", 87, 85, 71, -4, -6, "HOU", "MAF", 441, 2, 14, 0, "",0, null, null, null, null, null),
				Row( 4, 2008, 1, 3, 4, 903, 855, 1203, 1205, "WN", 3, "N308SA", 120, 130, 108, -2, 8, "HOU", "MCO", 848, 5, 7, 0, "",0, null, null, null, null, null),
				Row( 5, 2008, 1, 3, 4, 1423, 1400, 1726, 1710, "WN", 25, "N462WN", 123, 130, 107, 16, 23, "HOU", "MCO", 848, 6, 10, 0, "",0, 16, 0, 0, 0, 0),
				Row( 6, 2008, 1, 3, 4, 2024, 2020, 2325, 2325, "WN", 51, "N483WN", 121, 125, 101, 0, 4, "HOU", "MCO", 848, 13, 7, 0, "",0, null, null, null, null, null),
				Row( 7, 2008, 1, 3, 4, 1753, 1745, 2053, 2050, "WN", 940, "N493WN", 120, 125, 107, 3, 8, "HOU", "MCO", 848, 6, 7, 0, "",0, null, null, null, null, null),
				Row( 8, 2008, 1, 3, 4, 622, 620, 935, 930, "WN", 2621, "N266WN", 133, 130, 107, 5, 2, "HOU", "MCO", 848, 7, 19, 0, "",0, null, null, null, null, null),
				Row( 9, 2008, 1, 3, 4, 1944, 1945, 2210, 2215, "WN", 389, "N266WN", 146, 150, 124, -5, -1, "HOU", "MDW", 937, 7, 15, 0, "",0, null, null, null, null, null)
			);
            val headers = List("RecordId", "Year", "Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")

            // Generate an appropriate schema
            val schema = StructType( StructField( headers(0),  IntegerType, false ) ::
                                     StructField( headers(1),  IntegerType,  true ) ::
                                     StructField( headers(2),  IntegerType,  true ) ::
                                     StructField( headers(3),  IntegerType,  true ) ::
                                     StructField( headers(4),  IntegerType,  true ) ::
                                     StructField( headers(5),  IntegerType,  true ) ::
                                     StructField( headers(6),  IntegerType,  true ) ::
                                     StructField( headers(7),  IntegerType,  true ) ::
                                     StructField( headers(8),  IntegerType,  true ) ::
                                     StructField( headers(9),  StringType,   true ) ::
                                     StructField( headers(10), IntegerType,  true ) ::
                                     StructField( headers(11), StringType,   true ) ::
                                     StructField( headers(12), IntegerType,  true ) ::
                                     StructField( headers(13), IntegerType,  true ) ::
                                     StructField( headers(14), IntegerType,  true ) ::
                                     StructField( headers(15), IntegerType,  true ) ::
                                     StructField( headers(16), IntegerType,  true ) ::
                                     StructField( headers(17), StringType,   true ) ::
                                     StructField( headers(18), StringType,   true ) ::
                                     StructField( headers(19), IntegerType,  true ) ::
                                     StructField( headers(20), IntegerType,  true ) ::
                                     StructField( headers(21), IntegerType,  true ) ::
                                     StructField( headers(22), IntegerType,  true ) ::
                                     StructField( headers(23), StringType,   true ) ::
                                     StructField( headers(24), IntegerType,  true ) ::
                                     StructField( headers(25), IntegerType,  true ) ::
                                     StructField( headers(26), IntegerType,  true ) ::
                                     StructField( headers(27), IntegerType,  true ) ::
                                     StructField( headers(28), IntegerType,  true ) ::
                                     StructField( headers(29), IntegerType,  true ) :: Nil );
            val df = createDataFrame( data, schema );
            logger.debug(s"Created a dataframe with	${df.count} rows");

            val tableName = "TEST"

			// Use unknown protocol 'ptth' in url and jdbc.url values
			val options_sql = Map( "database.url" -> "ptth://localhost:9191",
				"database.jdbc_url" -> s"jdbc:kinetica:URL=ptth://localhost:9191",
				"ingester.analyze_data_only" -> "true",
				"ingester.flatten_source_schema" -> "false",
				"table.name" -> tableName,
				"table.create" -> "false")

			// Run the ingest with dry-run option to generate table DDL without connecting to Kinetica
			df.write.format("com.kinetica.spark").options(options_sql).save()
			logger.debug("Dry run completed successfully without connecting to non-existent Kinetica instance");

			try {
				// Attempt to connect to provided Kinetica instance with provided unknown protocol
				logger.debug("Attempt to connect to non-existent Kinetica with bad protocol ptth.");
				val table_size = get_table_size( tableName );
			} catch {
				 case e: com.gpudb.GPUdbException =>
				 case e: com.kinetica.spark.util.table.KineticaException =>
                 case e: java.lang.RuntimeException => {
                    logger.debug( s"Caught this exception connecting to non-existent Kinetica: {}", e );
                    assert( (e.toString() contains "unknown protocol") || (e.toString() contains "connection refused"),
                    		s"Connecting to non-existent Kinetica instance should fail." );
                }
			}
		}

    }   // end tests for kineticaExamples


}   // end trait SparkConnectorExamplesBehavior



/**
 *  Test examples using the DataSource v1 package.
 */
class TestExamples_V1
    extends SparkConnectorTestBase
            with SparkConnectorExamplesBehavior {

    override val m_package_descr   = m_v1_package_descr;
    override val m_package_to_test = m_v1_package;

    // Run the tests
    testsFor( kineticaExamples( m_package_to_test, m_package_descr ) );

}  // TestExamples_V1



/**
 *  Test examples using the DataSource v2 package.
 */
class TestExamples_V2
    extends SparkConnectorTestBase
            with SparkConnectorExamplesBehavior {

    override val m_package_descr   = m_v2_package_descr;
    override val m_package_to_test = m_v2_package;

    // Run the tests
    testsFor( kineticaExamples( m_package_to_test, m_package_descr ) );

}  // end TestExamples_V2
