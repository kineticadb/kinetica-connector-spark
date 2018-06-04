package com.kinetica.spark.util

import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp
import java.util.Iterator

import scala.beans.BeanProperty
import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gpudb.BulkInserter
import com.gpudb.GPUdb
import com.gpudb.GPUdbBase
import com.gpudb.GPUdbException
import com.gpudb.GenericRecord
import com.gpudb.Type
import com.kinetica.spark.LoaderParams
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConversions._

object KineticaSparkDFManager extends LazyLogging {

    @BeanProperty
    var df: DataFrame = null
    
    var myType: Type = null
    
    @BeanProperty
    var sc: JavaSparkContext = _

    /**
     * Methods returns kinetica table type
     * If table type is not set, method will call setType
     * @param lp Loader params
     * @return kinetica table type
     */
    def getType(lp: LoaderParams): Type = {
        if (myType == null) {
            setType(lp)
        }
        myType
    }

    def setType(lp: LoaderParams): Unit = {
        try {
            //println("Kinetica URL is " + lp.getKineticaURL)
            val gpudb: GPUdb = new GPUdb(lp.getKineticaURL, getGPUDBOptions(lp))
            //println(" Attempting Type.fromTable for table name " + lp.getTablename)
            myType = Type.fromTable(gpudb, lp.getTablename)
        } catch {
            case e: GPUdbException => e.printStackTrace()
        }
    }

    /**
     * Returns Kinetica table type
     * Use with care.  Does not call set type method if type is not set
     * @return Kinetica table type
     */
    def getType(): Type = myType

    private def getGPUDBOptions(lp: LoaderParams): GPUdbBase.Options = {
        val opts: GPUdbBase.Options = new GPUdbBase.Options
        logger.debug("Setting username and password")
        opts.setUsername(lp.getKusername.trim())
        opts.setPassword(lp.getKpassword.trim())
        opts.setThreadCount(lp.getThreads)
        opts
    }

    def toDouble: (Any) => Double = { case i: Int => i case f: Float => f case d: Double => d }

    /**
     * Maps over dataframe using either matching columns or chronological ordering
     * @param lp LoaderParams
     */
    def KineticaMapWriter(lp: LoaderParams): Unit = {
        logger.info("KineticaMapWriter")
        val typef: Type = myType
        val bkp: LoaderParams = lp
        if (lp.isMapToSchema) {
            logger.debug("Mapping Dataset columns to Kinetica")
            df.foreachPartition(new ForeachPartitionFunction[Row]() {
                def call(t: Iterator[Row]): Unit = {
                    val kbl: KineticaBulkLoader = new KineticaBulkLoader(bkp)
                    val bi: BulkInserter[GenericRecord] = kbl.GetBulkInserter()
                    var isARecord: Boolean = false
                    while (t.hasNext) {
                        val row: Row = t.next()
                        val genericRecord: GenericRecord = new GenericRecord(typef)
                        for (column <- typef.getColumns) {
                            try {
                                val rtemp: AnyRef = row.getAs(column.getName)
                                if (rtemp != null) {
                                    logger.debug("object not null")
                                    if (rtemp.isInstanceOf[Timestamp]) {
                                        logger.debug("Timestamp instance")
                                        genericRecord.put(
                                            column.getName,
                                            classOf[Timestamp].cast(rtemp).getTime)
                                        isARecord = true
                                    } else if (rtemp.isInstanceOf[java.lang.Boolean]) {
                                        logger.debug("Boolean instance")
                                        if (classOf[Boolean].cast(rtemp).booleanValue()) {
                                            logger.debug("Cast to 1")
                                            genericRecord.put(column.getName, 1)
                                            isARecord = true
                                        } else {
                                            logger.debug("Cast to 0")
                                            genericRecord.put(column.getName, 0)
                                            isARecord = true
                                        }
                                    } else if (rtemp.isInstanceOf[BigDecimal]) {
                                        logger.debug("BigDecimal")
                                        genericRecord.put(
                                            column.getName,
                                            classOf[BigDecimal].cast(rtemp).doubleValue())
                                        isARecord = true
                                    } else if (rtemp.isInstanceOf[java.lang.Short]) {
                                        logger.debug("Short")
                                        genericRecord.put(
                                            column.getName,
                                            classOf[Short].cast(rtemp).intValue())
                                        isARecord = true
                                    } else if (rtemp.isInstanceOf[java.lang.Byte]) {
                                        logger.debug("Byte")
                                        genericRecord.put(
                                            column.getName,
                                            classOf[Byte].cast(rtemp).intValue())
                                        isARecord = true
                                    } else if (rtemp.isInstanceOf[Date]) {
                                        logger.debug("DateType")
                                        genericRecord.put(
                                            column.getName,
                                            classOf[Date].cast(rtemp).toString)
                                        isARecord = true
                                    } else {
                                        logger.debug("Spark type {} ", rtemp.getClass())
                                        logger.debug("Kin instance type is {} {}", column.getType, column.getName)
                                        genericRecord.put(
                                            column.getName,
                                            column.getType.cast(rtemp))
                                        isARecord = true
                                    }
                                }
                                /*
                                else if (column.getType().isInstance(rtemp)) {
                                    log.debug("other instance");
                                    genericRecord.put(column.getName(), column.getType().cast(rtemp));
                                    isARecord=true;
                                }*/

                                /*
                                else if (column.getType().isInstance(rtemp)) {
                                    log.debug("other instance");
                                    genericRecord.put(column.getName(), column.getType().cast(rtemp));
                                    isARecord=true;
                                }*/
                            } catch {
                                case iae: IllegalArgumentException =>
                                    iae.printStackTrace()
                                    logger.warn("Found non-matching column DS.column --> KineticaTable.column, moving on", iae)
                                    throw iae
                                case e: Exception =>
                                    e.printStackTrace()
                                    throw e

                            }
                        }
                        if (isARecord) {
                            bi.insert(genericRecord)
                            //reset for next record
                            isARecord = false
                        }
                    }
                    try bi.flush()
                    catch {
                        case e: Exception => {
                            logger.error("Flush error", e)
                            e.printStackTrace()
                        }
                    }
                }
            })
        } else {
            logger.debug("Mapping chronological column order Kinetica table")
            df.foreachPartition(new ForeachPartitionFunction[Row]() {
                def call(t: Iterator[Row]): Unit = {
                    val kbl: KineticaBulkLoader = new KineticaBulkLoader(bkp)
                    val bi: BulkInserter[GenericRecord] = kbl.GetBulkInserter()
                    var isARecord: Boolean = false
                    while (t.hasNext) {
                        val row: Row = t.next()
                        val genericRecord: GenericRecord = new GenericRecord(typef)
                        var i: Int = 0
                        for (column <- typef.getColumns) {
                            val rtemp: Any = row.get({ i += 1; i - 1 })
                            if (rtemp != null) {
                                logger.debug("object not null " + rtemp)
                                if (rtemp.isInstanceOf[java.sql.Timestamp]) {
                                    logger.debug("Timestamp instance")
                                    genericRecord.put(
                                        column.getName,
                                        classOf[java.sql.Timestamp].cast(rtemp).getTime)
                                    isARecord = true
                                } else if (rtemp.isInstanceOf[java.sql.Date]) {
                                    logger.debug("Date instance")
                                    genericRecord.put(column.getName, rtemp.toString)
                                    isARecord = true
                                } else if (rtemp.isInstanceOf[java.lang.Boolean]) {
                                    logger.debug("Boolean instance")
                                    if (classOf[Boolean].cast(rtemp).booleanValue()) {
                                        logger.debug("Cast to 1")
                                        genericRecord.put(column.getName, 1)
                                        isARecord = true
                                    } else {
                                        logger.debug("Cast to 0")
                                        genericRecord.put(column.getName, 0)
                                        isARecord = true
                                    }
                                } else if (rtemp.isInstanceOf[BigDecimal]) {
                                    logger.debug("BigDecimal")
                                    genericRecord.put(
                                        column.getName,
                                        classOf[BigDecimal].cast(rtemp).doubleValue())
                                    isARecord = true
                                } else if (rtemp.isInstanceOf[java.lang.Short]) {
                                    logger.debug("Short")
                                    genericRecord.put(
                                        column.getName,
                                        classOf[Short].cast(rtemp).intValue())
                                    isARecord = true
                                } else if (rtemp.isInstanceOf[java.lang.Byte]) {
                                    logger.debug("Byte")
                                    genericRecord.put(
                                        column.getName,
                                        classOf[Byte].cast(rtemp).intValue())
                                    isARecord = true
                                } else if (rtemp.isInstanceOf[java.lang.Double]) {
                                    logger.debug("Double and column cannon name = " + column.getType.getCanonicalName)
                                    if (column.getType().getCanonicalName.equals("java.lang.Double")) {
                                        logger.debug("Double handling - putting double")
                                        genericRecord.put(column.getName, rtemp)
                                    } else {
                                        logger.debug("Double handling - putting float")
                                        val f1 = toDouble(rtemp)
                                        genericRecord.put(column.getName, f1.toFloat)
                                    }
                                    isARecord = true
                                } else if (rtemp.isInstanceOf[java.lang.Long]) {
                                    logger.debug("Long")
                                    genericRecord.put(
                                        column.getName,
                                        rtemp)
                                    isARecord = true
                                } else if (rtemp.isInstanceOf[java.lang.String]) {
                                    logger.debug("String found, column type is " + column.getType + " for name " + column.getName)
                                    // This is the path most travelled....
                                    if (column.getType().toString().contains("java.lang.Double")) {
                                        genericRecord.put(column.getName, rtemp.toString().toDouble)
                                    } else if (column.getType().toString().contains("java.lang.Float")) {
                                        genericRecord.put(column.getName, rtemp.toString().toFloat)
                                    } else if (column.getType().toString().contains("java.lang.Integer")) {
                                        genericRecord.put(column.getName, rtemp.toString().toInt)
                                    } else if (column.getType().toString().contains("java.lang.Long")) {
                                        genericRecord.put(column.getName, rtemp.toString().toLong)
                                    } else {
                                        genericRecord.put(column.getName, rtemp.toString())
                                    }
                                    isARecord = true
                                } else {
                                    logger.debug("Spark type and value {} {}", rtemp.getClass(), rtemp)
                                    logger.debug("Kin instance " + column.getType)
                                    genericRecord.put(column.getName, rtemp)
                                    isARecord = true
                                }
                            }
                            /*
                            else if (column.getType().isInstance(rtemp)) {
                                log.debug("other instance");
                                genericRecord.put(column.getName(), column.getType().cast(rtemp));
                                isARecord=true;
                            }*/
            
                                        /*
                            else if (column.getType().isInstance(rtemp)) {
                                log.debug("other instance");
                                genericRecord.put(column.getName(), column.getType().cast(rtemp));
                                isARecord=true;
                            }*/
                        }
                        i = 0
                        if (isARecord) {
                            bi.insert(genericRecord)
                            //reset for next record
                            isARecord = false
                        }
                    }
                    try bi.flush()
                    catch {
                        case e: Exception => {
                            e.printStackTrace()
                            logger.error("Flush error", e)
                        }

                    }
                }
            })
        }
    }
}
/*
if( maptoschem ) {
	Kinetica type t
	for each row r {
		for each col c in t {
			colValue = r.getAs(c.getName)
			gr.put(c.getName, cast(colValue))
		}
	}
} else {
	Kinetica type t
	for each row r {
		int cnt = 0;
		for each col c in t {
			colValue = r.get(cnt++)
			gr.put(c.getName, cast(colValue))
		}
	}			
}
*/
