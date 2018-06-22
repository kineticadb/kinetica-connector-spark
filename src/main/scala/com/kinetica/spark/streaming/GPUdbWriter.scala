package com.kinetica.spark.streaming

import com.gpudb._
import com.gpudb.protocol.InsertRecordsRequest
import java.io.Serializable
import java.util.ArrayList
import java.util.Iterator
import java.util.List
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.Function
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.streaming.api.java._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.LazyLogging

import com.kinetica.spark.LoaderParams

//remove if not needed
import scala.collection.JavaConversions._

object GPUdbWriter {

}

/**
 * GPUdb data processor, used in accepting object records or parseable string
 * records of a given type and inserting them into the database
 *
 * @param <T> type of RecordObject to insert into GPUdb
 */
@SerialVersionUID(-5273795273398765842L)
class GPUdbWriter[T <: Record] (lp : LoaderParams ) extends Serializable with LazyLogging {
  
    private val threads: Int = 4
    private val tableName: String = lp.tablename
    private val insertSize: Int = lp.insertSize

    private var records: List[T] = new ArrayList[T]()

    if (lp.tablename == null || lp.tablename.isEmpty())
        throw new IllegalArgumentException("No database url defined")

    /**
     * Writes the contents of an RDD to GPUdb
     *
     * @param rdd RDD to write to GPUdb
     */
    def write(rdd: JavaRDD[T]): Unit = {
        rdd.foreachPartition(new VoidFunction[Iterator[T]]() {
            private val serialVersionUID: Long = 1519062387719363984L

            override def call(tSet: Iterator[T]): Unit = {
                while (tSet.hasNext) {
                    val t: T = tSet.next()
                    if (t != null) write(t)
                }
                flush()
            }
        })
    }

    /**
     * Writes the contents of a Spark stream to GPUdb
     *
     * @param dstream data stream to write to GPUdb
     */
    def write(dstream: JavaDStream[T]): Unit = {
        dstream.foreachRDD(new VoidFunction[JavaRDD[T]]() {
            private val serialVersionUID: Long = -6215198148637505774L

            override def call(rdd: JavaRDD[T]): Unit = {
                rdd.foreachPartition(new VoidFunction[Iterator[T]]() {
                    private val serialVersionUID: Long = 1519062387719363984L

                    override def call(tSet: Iterator[T]): Unit = {
                        while (tSet.hasNext) {
                            val t: T = tSet.next()
                            if (t != null) write(t)
                        }
                    }
                })
            }
        })
    }
    
    /**
     * Writes a record to GPUdb
     *
     * @param t record to write to GPUdb
     */
    def write(t: T): Unit = {
        records.add(t)
        logger.debug("Added <{}> to write queue", t)
        if (records.size >= insertSize) flush()
    }
    
    /**
     * Flushes the set of accumulated records, writing them to GPUdb
     */
    def flush(): Unit = {
        try {
            logger.debug("Creating new GPUdb...")
            val gpudb: GPUdb = lp.getGpudb()
            val recordsToInsert: List[T] = records
            records = new ArrayList[T]()
            logger.info(
                "Writing <{}> records to table <{}>",
                recordsToInsert.size,
                tableName)
            for (record <- recordsToInsert) {
                val out = record
                logger.debug(" Array Item: <{}>", out)
            }
            val insertRequest: InsertRecordsRequest[T] =
                new InsertRecordsRequest[T]()
            insertRequest.setData(recordsToInsert)
            insertRequest.setTableName(tableName)
            gpudb.insertRecords(insertRequest)
        } catch {
            case ex: Exception => logger.error("Problem writing record(s)", ex)
        }
    }

}
