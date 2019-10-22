package com.kinetica.spark.egressutil

import java.sql.Connection
import org.apache.spark.Partition
import org.apache.spark.sql.sources._
import scala.collection.mutable.ArrayBuffer

/**
 * Get information about data slices.
 */
private[kinetica] object KineticaInputFormat {

    /**
     * Get number of data slices configured in the database.
     * @param conn connection to the database.
     * @return number of of data slices
     */
    def getTotalNumberOfRows(conn: Connection, table: String, filters: Array[Filter]): Long = {

        // query to get maximum number of data slices in the database.
        val whereClause = KineticaFilters.getWhereClause(filters)
        // Need to quote the table name, but quotess don't work with string
        // interpolation in scala; the following is correct, though ugly
        var query = s"""select count(*) from "${table}""""
        if (whereClause.length > 0) {
            query += " " + whereClause
        }
        //println(" Priming query is " + query)
        val stmt = conn.prepareStatement(query)
        try {
            val rs = stmt.executeQuery()
            try {

                val numberOfRows = if (rs.next) rs.getLong(1) else 0
                /* What nonsense - kinetica filter may return 0 rows
                if (numberOfRows == 0) {
                    // there should always be some data slices with kinetica
                    throw new Exception("No data slice ids returned.");
                }
                */ 
                //println(s"Total rows in table ${table} is ${numberOfRows}")
                return numberOfRows
            } finally {
                rs.close()
            }
        } finally {
            stmt.close
        }
    }

    /**
     * Get partitions mapping to the data slices in the database,
     */
    def getDataSlicePartition(conn: Connection, numPartitions: Int, table: String,
                              filters: Array[Filter],
                              offset: Long, limit: java.lang.Long ): Array[Partition] = {
        var numberOfRows = getTotalNumberOfRows(conn, table, filters)
        // If the user provided a limit, use that instead of ALL the records
        if ( limit != null ) {
            numberOfRows = limit;
        }
        var rowsPerPartition = numberOfRows / numPartitions
        
        val extra = numberOfRows % numPartitions

        /*
        println(s"Number of rows is ${numberOfRows}") 
        println(s"Rows per partition is ${rowsPerPartition}") 
        println(s"Number of partitions is ${numPartitions}") 
		*/
        
        if (rowsPerPartition <= 1) {
            Array[Partition](KineticaPartition( offset, numberOfRows, 0))
        } else {
            val ans = new ArrayBuffer[Partition]()
            var start : Long   = offset
            var numRows : Long = rowsPerPartition
            for (index <- 0 until numPartitions) {
                if( index == (numPartitions-1) ) {
                    numRows = numRows + extra
                }
                //println(s"Partition is ${start}/${numRows}/${index}") 
                ans += KineticaPartition(start, numRows, index)
                start = start + numRows;
            }
            ans.toArray
        }
    }
}

