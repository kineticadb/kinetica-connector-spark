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
    def getTotalNumberOfRows(conn: Connection, table: String, filters: Array[Filter]): Integer = {

        // query to get maximum number of data slices in the database.
        val whereClause = KineticaFilters.getWhereClause(filters)
        var query = s"select count(*) from ${table}"
        if (whereClause.length > 0) {
            query += " " + whereClause
        }
        //println(" Priming query is " + query)
        val stmt = conn.prepareStatement(query)
        try {
            val rs = stmt.executeQuery()
            try {

                val numberOfRows = if (rs.next) rs.getInt(1) else 0
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
    def getDataSlicePartition(conn: Connection, numPartitions: Int, table: String, filters: Array[Filter]): Array[Partition] = {
        val numberOfRows = getTotalNumberOfRows(conn, table, filters)
        var rowsPerPartition = numberOfRows / numPartitions
        
        if( numberOfRows % numPartitions != 0 ) {
            rowsPerPartition += 1     
        }
        
        //println(s"Number of rows is ${numberOfRows}") 
        //println(s"Rows per partition is ${rowsPerPartition}") 
        //println(s"Number of partitions is ${numPartitions}") 
        
        if (rowsPerPartition <= 1) {
            Array[Partition](KineticaPartition(0, numberOfRows, 0))
        } else {
            val ans = new ArrayBuffer[Partition]()
            var partitionIndex = 0
            var start = 0
            var numRows = rowsPerPartition
            for (index <- 1 to numPartitions) {
                //println(s"Partition is ${start}/${numRows}/${partitionIndex}") 
                ans += KineticaPartition(start, numRows, partitionIndex)
                partitionIndex = partitionIndex + 1
                start = start + numRows;
            }
            ans.toArray
        }
    }
}

