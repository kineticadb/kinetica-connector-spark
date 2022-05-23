package com.kinetica.spark.util

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.sql.DatabaseMetaData
import java.util.Properties
import com.kinetica.spark.LoaderParams
import com.typesafe.scalalogging.LazyLogging

object JDBCConnectionUtils extends LazyLogging {

    private var connection: Connection = null

    private var stmt: Statement = null

    /**
     * Initialize JDBCConnectionUtils
     * @param lp  LoaderParams
     */
    def Init(lp: LoaderParams): Unit = {
        connection = createKineticaConnection(lp)
        //set this connection to the local variable, so that we can close it later.
        stmt = connection.createStatement()
    }

    private def createKineticaConnection(lp: LoaderParams): Connection =
        getConnection("com.kinetica.jdbc.Driver", lp)

    private def getConnection(driverClass: String, lp: LoaderParams): Connection = {
        Class.forName(driverClass)
        val p: Properties = new Properties()
        p.setProperty("UID", lp.getKusername)
        p.setProperty("PWD", lp.getKpassword)
        DriverManager.getConnection(lp.getJdbcURL, p)
    }

    /**
     * Close connection and statement
     */
    def close(): Unit = {
        if (stmt != null && !stmt.isClosed) {
            stmt.close()
        }
        if (connection != null && !connection.isClosed) {
            connection.close()
        }
    }

    def executeSQL(sqlStatement: String): Unit = {
        try {
            stmt.execute(sqlStatement)
            stmt.getResultSet
            stmt.close()
        } catch {
            case e: Exception => {
                e.printStackTrace()
                throw new RuntimeException("SQL failed: " + sqlStatement, e)
            }
        }
    }

    def tableExists(tableName: String): Boolean = {
        val tableNameMinusSchema = if (tableName.split("\\.").length == 2 ) tableName.split("\\.")(1) else tableName

        val dbm = connection.getMetaData;
        val tables = dbm.getTables(null, null, tableNameMinusSchema, null);
        if (tables.next()) {
            true
        } else {
            false
        }
    }
}
