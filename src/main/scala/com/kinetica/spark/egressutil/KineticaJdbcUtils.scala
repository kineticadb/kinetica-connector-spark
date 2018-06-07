package com.kinetica.spark.egressutil

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.sources.Filter
import org.slf4j.LoggerFactory

/**
  * Utility jdbc methods to communicate with Kinetica. These methods are based on Spark SQL code,
  * and depends on the internal class DriverRegistry.
  */
private[kinetica] object KineticaJdbcUtils {

  private val log = LoggerFactory.getLogger(getClass)

  /**
    * Given  an url, return a function that loads the
    * specified driver string then returns a connection to the JDBC url.
    * getConnector is run on the driver code, while the function it returns
    * is run on the executor.
    *
    * @param url - The JDBC url to connect to.
    *
    * @return A function that loads the driver and connects to the url.
    */
  def getConnector(url: String, properties: Properties): () => Connection = {
    () => {
      val driver = "com.simba.client.core.jdbc4.SCJDBC4Driver"
      try {
        if (driver != null) DriverRegistry.register(driver)
      } catch {
        case e: ClassNotFoundException =>
          log.error(s"Couldn't find class $driver", e)
      }
      DriverManager.getConnection(url, properties)
    }
  }

  def quoteIdentifier(colName: String): String = {
    s"""$colName"""
  }

  def getCountWithFilter(
      url: String,
      properties: Properties,
      table: String,
      filters: Array[Filter]): Long = {
    val whereClause = KineticaFilters.getFilterClause(filters)
    val countQuery = s"SELECT count(*) FROM $table $whereClause"
    log.info(countQuery)
    val conn = KineticaJdbcUtils.getConnector(url, properties)
    var count: Long = 0
    try {
      val results = conn().prepareStatement(countQuery).executeQuery()

      if (results.next()) {
        count = results.getLong(1)
      }
      else {
        throw new IllegalStateException("Could not read count from Kinetica")
      }
    } finally {
      conn().close()
    }
    count
  }
}
