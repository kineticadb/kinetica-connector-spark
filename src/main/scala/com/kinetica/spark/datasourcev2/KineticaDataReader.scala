package com.kinetica.spark.datasourcev2

import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import com.kinetica.spark.LoaderParams
import com.kinetica.spark.util.ConfigurationConstants._
import com.typesafe.scalalogging.Logger

class KineticaDataReader (
    val conf: LoaderParams,
    val tableSchema: StructType,
    val pushedCatalystFilters: Array[Expression],
    val requiredSchema: StructType)
    extends DataReader[Row] {
 
    val logger = Logger("KineticaDataReader")

    val requiredColumns = requiredSchema.fieldNames

    val numPartitions = conf.getNumPartitions
    val url   = if (conf.getJdbcURL   != null ) conf.getJdbcURL   else sys.error("Option 'database.jdbc_url' not specified")
    val table = if (conf.getTablename != null ) conf.getTablename else sys.error("Option 'table.name' not specified")
        
    // // Handle the case when no required column is given
    // if ( requiredColumns.isEmpty ) {
    // }

    // Read the table's rows
    val myrows: Iterator[Row] = {
       val queryStr = buildTableQuery(table, requiredColumns, pushedCatalystFilters)

       val conn = com.kinetica.spark.egressutil.KineticaJdbcUtils.getConnector(url, conf)()
       val rs   = conn.prepareStatement( queryStr ).executeQuery
        
       val internalRows = com.kinetica.spark.egressutil.KineticaUtils.resultSetToSparkInternalRows( rs, requiredSchema )
       val encoder = org.apache.spark.sql.catalyst.encoders.RowEncoder.apply( requiredSchema ).resolveAndBind()
       internalRows.map(encoder.fromRow)
    }
  
  
    override def next(): Boolean = {
        myrows.hasNext
    }

    override def get(): Row = {
        myrows.next()
    }

    override def close(): Unit = {
    }
  
    private def buildTableQuery(
        table: String,
        columns: Array[String],
        pushedCatalystFilters: Array[Expression]
        ): String = {

        val baseQuery = {

            val colStrBuilder = new StringBuilder()

            //log.info(" KineticaDataReader buildTableQuery are - ")
            //columns.foreach(println)

            if (columns.length > 0) {
                colStrBuilder.append(columns(0))
                    columns.drop(1).foreach(col => colStrBuilder.append(",").append(col))
            } else {
                colStrBuilder.append("1")
            }

            val whereClause = if (pushedCatalystFilters.size > 0) {
                val sb = new StringBuilder("WHERE ")
                pushedCatalystFilters.foreach(x => sb.append(x.sql).append(" AND "))
                sb.substring(0, sb.length - 5) // remove the trailing AND
                } else ""
            // val whereClause = ""

            s"SELECT $colStrBuilder FROM $table $whereClause"
        }
        logger.info("External Table Query: " + baseQuery)
        baseQuery.toString()
    }   // end buildTableQuery


}  // end class KineticaDataReader
