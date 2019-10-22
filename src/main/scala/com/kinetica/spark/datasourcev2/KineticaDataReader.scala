package com.kinetica.spark.datasourcev2;

import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import com.kinetica.spark.LoaderParams;
import com.kinetica.spark.egressutil.KineticaEgressUtilsNativeClient;
import com.kinetica.spark.egressutil.KineticaEgressUtilsJdbc;
import com.kinetica.spark.util.ConfigurationConstants._;
// import com.typesafe.scalalogging.Logger;
import com.typesafe.scalalogging.LazyLogging;

    
class KineticaDataReader (
    val conf: LoaderParams,
    val tableSchema: StructType,
    val pushedCatalystFilters: Array[Expression],
    val requiredSchema: StructType)
    extends DataReader[Row]
    with LazyLogging {
 
    // val logger = Logger("KineticaDataReader");
  
    val requiredColumns = requiredSchema.fieldNames;

    val numPartitions = conf.getNumPartitions;
    val url   = if (conf.getJdbcURL   != null ) conf.getJdbcURL   else sys.error("Option 'database.jdbc_url' not specified");
    val table = if (conf.getTablename != null ) conf.getTablename else sys.error("Option 'table.name' not specified");
        
    // // Handle the case when no required column is given
    // if ( requiredColumns.isEmpty ) {
    // }


    val gpudbConn = conf.getGpudb;
    val tableName = conf.getTablename;
    
    // Get the table size
    val tableSize = KineticaEgressUtilsNativeClient.getKineticaTableSize( gpudbConn, tableName );
    logger.debug("KineticaDataReader: got table size {}", tableSize);
     
    // Read the table's rows
    val batchSize = 10000;

    // Offset and limit for getting data out
    val offset = conf.getEgressOffset;
    var limit  = conf.getEgressLimit;
    logger.debug( s"KineticaDataReader constructor: offset $offset limit $limit" );
    if ( limit == null ) {
        // Use the table size as the limit
        limit = tableSize;
    }
        
    
    // Retrieve the records
    val myRows: Iterator[Row] = {

        if ( pushedCatalystFilters.isEmpty ) {
            logger.debug("KineticaDataReader: No filters given; use the native client API");
            // Use the fastest path via the native Java API to get the records
            // since no filters are used
            KineticaEgressUtilsNativeClient.getRowsFromKinetica( gpudbConn, tableName,
                                                                 requiredColumns, requiredSchema,
                                                                 offset, limit, batchSize );
        } else {
            logger.debug("KineticaDataReader: Filters ARE given; use the JDBC connector");
            // Use the JDBC connector to get the records
            val queryStr = buildTableQuery( table, requiredColumns, pushedCatalystFilters,
                                            offset, limit );

            val conn = com.kinetica.spark.egressutil.KineticaJdbcUtils.getConnector(url, conf)();
            val stmt = conn.prepareStatement( queryStr );
            val rs = stmt.executeQuery;
        
            val internalRows = KineticaEgressUtilsJdbc.resultSetToSparkInternalRows( rs, requiredSchema );
            val encoder = org.apache.spark.sql.catalyst.encoders.RowEncoder.apply( requiredSchema ).resolveAndBind();
            internalRows.map( encoder.fromRow );
        }
    }

  
    override def next(): Boolean = {
        myRows.hasNext
    }

    override def get(): Row = {
        myRows.next()
    }

    override def close(): Unit = {
    }

    
    private def buildTableQuery(
        table: String,
        columns: Array[String],
        pushedCatalystFilters: Array[Expression],
        offset: Long,
        limit: Long
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

            // Need to quote the table name, but quotess don't work with string
            // interpolation in scala; the following is correct, though ugly
            s"""SELECT $colStrBuilder FROM "$table" $whereClause LIMIT $offset,$limit"""
        }
        logger.info("Query for retrieving records: " + baseQuery)
        baseQuery.toString()
    }   // end buildTableQuery


}  // end class KineticaDataReader
