package com.kinetica.spark.loader

import java.time.Duration
import java.time.Instant
import java.util.Date
import java.util.HashMap
import java.util.Iterator
import java.util.List

//remove if not needed
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.sql.Row

import com.gpudb.BulkInserter
import com.gpudb.GenericRecord
import com.gpudb.Type
import com.gpudb.Type.Column
import com.kinetica.spark.util.KineticaBulkLoader
import com.typesafe.scalalogging.LazyLogging

@SerialVersionUID(-594351038800346275L)
class KineticaLoaderFunction (
    private val loaderConfig: LoaderConfiguration,
    private val columnMap: HashMap[Integer, Integer])
        extends ForeachPartitionFunction[Row] with LazyLogging {

    private val tableType: Type = this.loaderConfig.getType

    override def call(rowset: Iterator[Row]): Unit = {

        val kbl: KineticaBulkLoader = new KineticaBulkLoader(loaderConfig)
        val bi: BulkInserter[GenericRecord] = kbl.GetBulkInserter()
        //val bi: BulkInserter[GenericRecord] = this.loaderConfig.getBulkInserter
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

    private def convertValue(inValue: Any, destColDef: Column): Any = {
        if (inValue == null) {
            return null
        }

        val destType: Class[_] = destColDef.getType
        val srcType: Class[_] = inValue.getClass
        val colProps: List[String] = destColDef.getProperties
        var outValue: Any = null

        // need to check timestamp first because it may need normalization
        if (colProps.contains("timestamp")) {
            outValue = convertFromDate(srcType, inValue);
        }
        else if (destType == srcType) {
            // fast path
            outValue = inValue
        }
        else if (classOf[Number].isAssignableFrom(destType)
                && classOf[Number].isAssignableFrom(srcType)) {
            // numeric conversion
            outValue = convertFromNumber(destType, inValue.asInstanceOf[Number])
        }
        else if (destType == classOf[java.lang.Integer] && srcType == classOf[java.lang.Boolean]) {
            // boolean conversion
            val inBool: java.lang.Boolean = inValue.asInstanceOf[java.lang.Boolean]
            outValue = if (inBool) 1.underlying else 0.underlying
        }

        if (outValue == null) {
            throw new Exception(
                String.format("Could not convert from type: %s", destType.getName))
        }

        if(loaderConfig.truncateToSize && destType == classOf[java.lang.String]) {
            // truncate string if length exceeds charN max
            val outStr: String = outValue.asInstanceOf[java.lang.String]
            val charNParam: Option[String] = colProps.filter(x => x.startsWith("char")).headOption

            if(!charNParam.isEmpty) {
                val charMax: Int = charNParam.get.stripPrefix("char").toInt
                if(outStr.size > charMax) {
                    outValue = outStr.substring(0, charMax)
                }
            }
        }

        outValue
    }

    private def convertFromDate(destType: Class[_], inObject: Any): Long = {

        // Dates larger than this will fail in 6.1
        val MAX_DATE: Long = 29379542399999L
        val MIN_DATE: Long = -30610224000000L
        var dateVal: Long = 0L

        if(classOf[java.util.Date].isAssignableFrom(destType)) {
            val inDate: Date = inObject.asInstanceOf[java.util.Date]
            dateVal = inDate.getTime
        }
        else if(classOf[java.lang.Long].isAssignableFrom(destType)) {
            dateVal = inObject.asInstanceOf[java.lang.Long]
        }

        if (dateVal > MAX_DATE) {
            dateVal = MAX_DATE
        }
        else if (dateVal < MIN_DATE) {
            dateVal = MIN_DATE
        }

        dateVal
    }

    private def convertFromNumber(destType: Class[_], inNumber: Number): Number = {
        var outValue: Number = null
        if (destType == classOf[java.lang.Integer]) {
            outValue = inNumber.intValue()
        } else if (destType == classOf[java.lang.Long]) {
            outValue = inNumber.longValue()
        } else if (destType == classOf[java.lang.Float]) {
            outValue = inNumber.floatValue()
        } else if (destType == classOf[java.lang.Double]) {
            outValue = inNumber.doubleValue()
        }
        outValue
    }

}
