package com.kinetica.spark.egressutil

import java.sql.{ Connection, ResultSet, PreparedStatement }
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.{ Partition, SparkContext, TaskContext }

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.CompletionIterator

import com.kinetica.spark.LoaderParams
import com.gpudb.Record;

import com.gpudb.protocol.GetRecordsByColumnRequest;
import com.gpudb.protocol.GetRecordsByColumnResponse;



import scala.util.control.NonFatal

import com.typesafe.scalalogging.LazyLogging

/**
 * Data corresponding to one partition of a Kinetica RDD.
 */
private[kinetica] case class KineticaPartition(startRow: Long, numRows: Long, idx: Int) extends Partition with LazyLogging {
    override def index: Int = idx
}

/**
 * An RDD representing a table in a database accessed via JDBC.  Both the
 * driver code and the workers must be able to access the database; the driver
 * needs to fetch the schema while the workers need to fetch the data.
 */
private[kinetica] class KineticaRDD(
    sc: SparkContext,
    getConnection: () => Connection,
    schema: StructType,
    table: String,
    columns: Array[String],
    filters: Array[Filter],
    partitions: Array[Partition],
    properties: Properties,
    conf: LoaderParams
    )
    extends RDD[Row](sc, Nil) with LazyLogging {

    /**
     * Retrieve the list of partitions corresponding to this RDD.
     */
    override def getPartitions: Array[Partition] = partitions

    override def compute(thePart: Partition, context: TaskContext): Iterator[Row] = {

        val part = thePart.asInstanceOf[KineticaPartition]

        // If no filter is given, use the native Java API to get the
        // records out using the fastest path possible
        if ( filters.isEmpty ) {
            // No filter is given, so use the Java API to fetch data.  The reason
            // we use the Java API only in this case is because we cannot push down
            // spark sql filters via the Java API.
            logger.debug("KineticaRDD::compute(): No filters given; use the native path");

            // Fetch the rows (after filtering the data server-side)
            val allRowsForPartition = KineticaEgressUtilsNativeClient.getRowsFromKinetica( conf.getGpudb, conf.getTablename,
                                                                                           columns, schema,
                                                                                           part.startRow, part.numRows,
                                                                                           conf.getEgressBatchSize,
                                                                                           true );

            // Note: Beware of calling .size or other functions on the rows created above
            //       (even in a debug print); it may have unintended consequences. For example,
            //       such a debug here with ${myRows.size} in it caused allRows to be iterated;
            //       as a result, the RDD was ALWAYS thought to be empty.  Bottom line: do NOT
            //       iterate over the rows being returned.

            def close() {
            }
            context.addTaskCompletionListener { context => close(); }

            allRowsForPartition
        } else {
            // Use the JDBC connector to push down the filters
            logger.debug("KineticaRDD::compute(): Filters are given; use the JDBC connector");
            fetchRecordsViaJDBC( thePart, context );
        }
    }


    /**
     *  Use the JDBC connector and KineticaEgressUtilsJdbc to fetch records
     * (not using the Java API direclty)
     */
    def fetchRecordsViaJDBC(thePart: Partition, context: TaskContext): Iterator[Row] = {
        var closed = false
        var rs: ResultSet = null
        var conn: Connection = null

        conn = getConnection()

        val part = thePart.asInstanceOf[KineticaPartition]
        val query = buildTableQuery(conn, table, columns, filters, part, schema)
        logger.debug("Query is {}", query)
        val stmt = conn.prepareStatement(query)
        rs = stmt.executeQuery

        val internalRows = KineticaEgressUtilsJdbc.resultSetToSparkInternalRows(rs, schema)
        val encoder = RowEncoder.apply(schema).resolveAndBind()

        val myrows = internalRows.map(encoder.fromRow)

        def close() {

            if (closed) return
            try {
                if (null != rs) {
                    rs.close()
                }
            } catch {
                case e: Exception => logWarning("Exception closing resultset", e)
            }
            try {
                if (null != stmt) {
                    stmt.close()
                }
            } catch {
                case e: Exception => logWarning("Exception closing statement", e)
            }
            try {
                if (null != conn) {
                    if (!conn.isClosed && !conn.getAutoCommit) {
                        try {
                            conn.commit()
                        } catch {
                            case NonFatal(e) => logWarning("Exception committing transaction", e)
                        }
                    }
                    conn.close()
                }
                logDebug("closed connection")
            } catch {
                case e: Exception => logWarning("Exception closing connection", e)
            }
            closed = true
        }
        context.addTaskCompletionListener { context => close() }
        myrows
//            CompletionIterator[InternalRow, Iterator[InternalRow]](
//          new InterruptibleIterator(context, rowsIterator), close())
    }   // end fetchRecordsViaJDBC


    def buildTableQuery(
        conn: Connection,
        table: String,
        columns: Array[String],
        filters: Array[Filter],
        partition: KineticaPartition,
        schema: StructType): String = {

        val baseQuery = {
            val (whereClause, maxRowsToFetch) = KineticaFilters.getWhereClause(filters, partition)
            val colStrBuilder = new StringBuilder()

            //log.info(" KineticaDataReader buildTableQuery are - ")
            //columns.foreach(println)

            if (columns.length > 0) {
                colStrBuilder.append(columns(0))
                columns.drop(1).foreach(col => colStrBuilder.append(",").append(col))
            } else {
                colStrBuilder.append("1")
            }

            var quotedTableName = KineticaJdbcUtils.quoteTableName(table);
            // Need to quote the table name, but quotess don't work with string
            // interpolation in scala; the following is correct, though ugly
            s"""/* KI_HINT_MAX_ROWS_TO_FETCH($maxRowsToFetch) */SELECT $colStrBuilder FROM "$quotedTableName" $whereClause"""

        }
        log.debug("External Table Query: " + baseQuery)
        baseQuery.toString()
    }   // end buildTableQuery
}
