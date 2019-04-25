package com.kinetica.spark.egressutil

import java.sql.{ Connection, DriverManager, ResultSet }
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.sources.Filter

import com.kinetica.spark.LoaderParams

import com.typesafe.scalalogging.LazyLogging

/**
 * Utility jdbc methods to communicate with Kinetica. These methods are based on Spark SQL code,
 * and depends on the internal class DriverRegistry.
 */
private[kinetica] object KineticaJdbcUtils extends LazyLogging {

    /**
     * Given a URL, return a function that loads the
     * specified driver string then returns a connection to the JDBC url.
     * getConnector is run on the driver code, while the function it returns
     * is run on the executor.
     *
     * @param url - The JDBC url to connect to.
     *
     * @return A function that loads the driver and connects to the url.
     */
    def getConnector(url: String, lp: LoaderParams): () => Connection = {
        () =>
            {
                val driver = "com.kinetica.jdbc.Driver"
                try {
                    if (driver != null) DriverRegistry.register(driver)
                } catch {
                    case e: ClassNotFoundException =>
                        logger.error(s"Couldn't find class $driver", e)
                }
                val p: Properties = new Properties()
                p.setProperty("UID", lp.getKusername)
                p.setProperty("PWD", lp.getKpassword)
                DriverManager.getConnection(url, p)
            }
    }

    def quoteIdentifier(colName: String): String = {
        s"""$colName"""
    }

    /**
     * Given a database connection and a SQL query, executes the query and
     * returns a function that generates a result set.
     *
     * @param conn   The database connection.
     * @param query  The SQL query.
     * @return  A function that executes the query and returns a ResultSet object.
     * @throws A RuntimeException when the query yields no result set.
     */
    def executeSqlQuery( conn: Connection, query: String ) : () => ResultSet = {
        // Prepare and execute a sql statement
        conn.prepareStatement( query ).executeQuery
    }

    def getCountWithFilter(
        url: String,
        properties: LoaderParams,
        table: String,
        filters: Array[Filter]): Long = {
        val whereClause = KineticaFilters.getFilterClause(filters)
        // Need to quote the table name, but quotess don't work with string
        // interpolation in scala; the following is correct, though ugly
        val countQuery = s"""SELECT count(*) FROM "$table" $whereClause"""
        logger.info(countQuery)
        val conn = KineticaJdbcUtils.getConnector(url, properties)()
        var count: Long = 0
        try {
            val results = conn.prepareStatement( countQuery ).executeQuery()

            if (results.next()) {
                count = results.getLong(1)
            } else {
                throw new IllegalStateException("Could not read count from Kinetica")
            }
        } finally {
            conn.close()
        }
        count
    }


}
