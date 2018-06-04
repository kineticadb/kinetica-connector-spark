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

import scala.util.control.NonFatal

/**
 * Data corresponding to one partition of a Kinetica RDD.
 */
private[kinetica] case class KineticaPartition(startRow: Int, numRows: Int, idx: Int) extends Partition {
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
    properties: Properties)
    extends RDD[Row](sc, Nil) {

    /**
     * Retrieve the list of partitions corresponding to this RDD.
     */
    override def getPartitions: Array[Partition] = partitions

    override def compute(thePart: Partition, context: TaskContext): Iterator[Row] = {
        var closed = false
        var rs: ResultSet = null
        var conn: Connection = null

        conn = getConnection()

        val part = thePart.asInstanceOf[KineticaPartition]
        val stmt = conn.prepareStatement(buildTableQuery(conn, table, columns, filters, part, schema))
        rs = stmt.executeQuery

        val internalRows = KineticaUtils.resultSetToSparkInternalRows(rs, schema)
        val encoder = RowEncoder.apply(schema).resolveAndBind()

        /*
        println(" *************************** ")
        println(encoder)
        println(encoder.clsTag)
        println(encoder.flat)

        println(internalRows.size)

        //internalRows.foreach(println)

        for( ir <- internalRows ) {
            println(ir)
            println(encoder.fromRow(ir))
        }
        println(" *************************** ")
        *
        */
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
                logInfo("closed connection")
            } catch {
                case e: Exception => logWarning("Exception closing connection", e)
            }
            context.addTaskCompletionListener { context => close() }
            closed = true
        }

        myrows

        /*
        CompletionIterator[InternalRow, Iterator[InternalRow]](
          new InterruptibleIterator(context, rowsIterator), close())
        */
    }

    def buildTableQuery(
        conn: Connection,
        table: String,
        columns: Array[String],
        filters: Array[Filter],
        partition: KineticaPartition,
        schema: StructType): String = {

        val baseQuery = {
            val whereClause = KineticaFilters.getWhereClause(filters, partition)
            val colStrBuilder = new StringBuilder()

            //log.info(" KineticaDataReader buildTableQuery are - ")
            //columns.foreach(println)

            if (columns.length > 0) {
                colStrBuilder.append(columns(0))
                columns.drop(1).foreach(col => colStrBuilder.append(",").append(col))
            } else {
                colStrBuilder.append("1")
            }
            s"SELECT $colStrBuilder FROM $table $whereClause"
        }
        log.info("External Table Query: " + baseQuery)
        baseQuery.toString()
    }
}
