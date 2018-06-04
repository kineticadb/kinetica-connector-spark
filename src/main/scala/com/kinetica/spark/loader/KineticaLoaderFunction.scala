package com.kinetica.spark.loader

import java.time.Duration
import java.time.Instant
import java.util.Date
import java.util.HashMap
import java.util.Iterator
import java.util.List
import java.util.Map.Entry

import org.apache.spark.api.java.function.ForeachPartitionFunction

import org.apache.spark.sql.Row
import com.gpudb.BulkInserter
import com.gpudb.GenericRecord

import com.gpudb.Type
import com.gpudb.Type.Column

import com.typesafe.scalalogging.LazyLogging

//remove if not needed
import scala.collection.JavaConversions._

object KineticaLoaderFunction {

    private def convertValue(inValue: Any, destColDef: Column): Any = {
        val colProps: List[String] = destColDef.getProperties
        if (inValue == null) {
            return null
        }
        
        val destType: Class[_] = destColDef.getType
        val srcType: Class[_] = inValue.getClass
        
        var outValue: Any = null
        if (destType == srcType) {
            // fast path
            outValue = inValue
        } else if (classOf[Number].isAssignableFrom(destType) && classOf[Number]
            .isAssignableFrom(srcType)) {
            // numeric conversion
            outValue = convertFromNumber(destType, inValue.asInstanceOf[Number])
        } else if (colProps.contains("timestamp") && classOf[Date]
            .isAssignableFrom(srcType)) {
            // timestamp conversion
            val inTimestamp: Date = inValue.asInstanceOf[Date]
            outValue = inTimestamp.getTime.underlying
        } else if (destType == classOf[java.lang.Integer] && srcType == classOf[java.lang.Boolean]) {
            // boolean conversion
            val inBool: java.lang.Boolean = inValue.asInstanceOf[java.lang.Boolean]
            outValue = if (inBool) 1.underlying else 0.underlying
        }
        if (outValue == null) {
            throw new Exception(
                String.format("Could not convert from type: %s", destType.getName))
        }
        outValue
    }

    private def convertFromNumber(destType: Class[_], inNumber: Number): Number = {
        var outValue: Number = null
        if (destType == classOf[Integer]) {
            outValue = inNumber.intValue()
        } else if (destType == classOf[Long]) {
            outValue = inNumber.longValue()
        } else if (destType == classOf[Float]) {
            outValue = inNumber.floatValue()
        } else if (destType == classOf[Double]) {
            outValue = inNumber.doubleValue()
        }
        outValue
    }

}

import KineticaLoaderFunction._

@SerialVersionUID(-594351038800346275L)
class KineticaLoaderFunction(
    private val loaderConfig: LoaderConfiguration,
    private val columnMap: HashMap[Integer, Integer])
    extends ForeachPartitionFunction[Row] with LazyLogging {

    private val tableType: Type = this.loaderConfig.getType

    override def call(rowset: Iterator[Row]): Unit = {
        val bi: BulkInserter[GenericRecord] = this.loaderConfig.getBulkInserter
        val start: Instant = Instant.now()
        val rowcount: Long = insertRows(rowset, bi)
        val end: Instant = Instant.now()
        logger.info("Inserted rows={}", rowcount)
        logger.info("Inserted rows time={}", Duration.between(start, end))
            
    }

    private def insertRows(
        rowset: Iterator[Row],
        bi: BulkInserter[GenericRecord]): Long = {
        logger.info("Starting insert into table: {}", bi.getTableName)
        var rowcount: Long = 0
        while (rowset.hasNext) {
            val row: Row = rowset.next()
            try {
                val record: GenericRecord = convertRow(row)
                bi.insert(record)
            } catch {
                case ex: Exception => {
                    val msg = s"Row ${rowcount} conversion failed: ${row}"
                    logger.error(msg)
                    throw new Exception(msg, ex)
                }

            }
            { rowcount += 1; rowcount - 1 }
            if (rowcount % 10000 == 0) {
                logger.info("Inserted rows: {}", rowcount)
            }
        }
        bi.flush()
        rowcount
    }

    private def convertRow(row: Row): GenericRecord = {
        val record: GenericRecord = new GenericRecord(this.tableType)
        for ((key, value) <- this.columnMap) {
            val srcColIdx: Int = key
            val destColIdx: Int = value
            val colDef: Column = this.tableType.getColumn(destColIdx)
            val srcValue: Any = row.get(srcColIdx)
            val destValue: Any = convertValue(srcValue, colDef)
            record.put(destColIdx, destValue)
        }
        record
    }

}
