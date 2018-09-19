package com.kinetica.spark.util

import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp
import java.util.Iterator

import scala.beans.BeanProperty
import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.SparkContext
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
import org.apache.spark.Logging

import scala.collection.JavaConversions._

object KineticaSparkDFManager extends Logging {

    @BeanProperty
    var df: DataFrame = null

    var myType: Type = null

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
            logDebug("Kinetica URL is " + lp.getKineticaURL)
            val gpudb: GPUdb = lp.getGpudb
            logDebug(" Attempting Type.fromTable for table name " + lp.getTablename)
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

    def toDouble: (Any) => Double = { case i: Int => i case f: Float => f case d: Double => d }
    def toLong: (Any) => Long = { case i: Int => i }

    /**
     * Maps over dataframe using either matching columns or chronological ordering
     * @param lp LoaderParams
     */
    def KineticaMapWriter(sc: SparkContext, lp: LoaderParams): Unit = {

        logDebug("KineticaMapWriter")
        val typef: Type = myType
        val bkp: LoaderParams = lp
        logDebug("Mapping Dataset columns to Kinetica")
        df.foreachPartition( records => {
            logDebug("Records found....")
            val kbl: KineticaBulkLoader = new KineticaBulkLoader(bkp)
            val bi: BulkInserter[GenericRecord] = kbl.GetBulkInserter()
            records.foreach { row =>
                val genericRecord: GenericRecord = new GenericRecord(typef)
                var i: Int = 0
                for (column <- typef.getColumns) {
                    try {
                        var rtemp: Any = row.get({ i += 1; i - 1 })
                        if (lp.isMapToSchema) {
                            rtemp = row.getAs(column.getName)
                        }
                        if( rtemp != null ) { // This means null value - nothing to do.
                            if (!putInGenericRecord(genericRecord, rtemp, column)) {
                            }
                        }
                    } catch {
                        case e: Exception =>
                            //e.printStackTrace()
                            logWarning("Found non-matching column DS.column --> KineticaTable.column, moving on", e)
                            throw e
                    }
                }
                bi.insert(genericRecord)    
            }
            try bi.flush()
            catch {
                case e: Exception => {
                    logError("Flush error", e)
                    e.printStackTrace()
                }
            }
        })
    }
    
    def putInGenericRecord(genericRecord : GenericRecord, rtemp : Any, column : Type.Column ) : Boolean = {
        var isARecord: Boolean = false
        if (rtemp != null) {
        	logDebug("Spark data type not null, class is " + rtemp.getClass())
        	if (rtemp.isInstanceOf[java.lang.Long]) {
        		logDebug("Long")
        		genericRecord.put(column.getName, rtemp)
        		isARecord = true
        	} else if (rtemp.isInstanceOf[java.lang.Integer]) {
        		logDebug("handling Integer")
        		if (column.getType().toString().contains("java.lang.Integer")) {
        		    genericRecord.put(column.getName, rtemp)
	        		isARecord = true
        		} else if (column.getType().toString().contains("java.lang.Long")) {
        		    genericRecord.put(column.getName, toLong(rtemp))
	        		isARecord = true
        		} else if (column.getType().toString().contains("java.lang.String")) {
        		    genericRecord.put(column.getName, rtemp.toString())
	        		isARecord = true
        		} else if (column.getType().toString().contains("java.lang.Double")) {
        		    genericRecord.put(column.getName, toDouble(rtemp))
	        		isARecord = true
        		} else {
        		   logDebug("***** Kinetica column type is " + column.getType + " for name " + column.getName) 
        		}
        	} else if (rtemp.isInstanceOf[Timestamp]) {
        		logDebug("Timestamp instance")
        		genericRecord.put(column.getName, classOf[Timestamp].cast(rtemp).getTime)
        		isARecord = true
        	} else if (rtemp.isInstanceOf[java.sql.Date]) {
        		logDebug("Date instance")
        		genericRecord.put(column.getName, rtemp.toString)
        		isARecord = true
        	} else if (rtemp.isInstanceOf[java.lang.Boolean]) {
        		logDebug("Boolean instance")
        		if (classOf[Boolean].cast(rtemp).booleanValue()) {
        			logDebug("Cast to 1")
        			genericRecord.put(column.getName, 1)
        			isARecord = true
        		} else {
        			logDebug("Cast to 0")
        			genericRecord.put(column.getName, 0)
        			isARecord = true
        		}
        	} else if (rtemp.isInstanceOf[BigDecimal]) {
        		logDebug("BigDecimal")
        		genericRecord.put(column.getName, classOf[BigDecimal].cast(rtemp).doubleValue())
        		isARecord = true
        	} else if (rtemp.isInstanceOf[java.lang.Short]) {
        		logDebug("Short")
        		genericRecord.put(column.getName, classOf[Short].cast(rtemp).intValue())
        		isARecord = true
        	} else if (rtemp.isInstanceOf[java.lang.Float]) {
        		logDebug("Float")
        		if (column.getType().toString().contains("java.lang.Float")) {
        		    genericRecord.put(column.getName, classOf[java.lang.Float].cast(rtemp).floatValue())
	        		isARecord = true
        		} else if (column.getType().toString().contains("java.lang.Double")) {
        		    genericRecord.put(column.getName, toDouble(rtemp))
	        		isARecord = true
        		} else if (column.getType().toString().contains("java.lang.String")) {
        		    genericRecord.put(column.getName, rtemp.toString())
	        		isARecord = true
        		} else {
        		   logDebug("**** Kinetica column type is " + column.getType + " for name " + column.getName) 
        		}
        	} else if (rtemp.isInstanceOf[java.lang.Double]) {
        	    if (column.getType().toString().contains("java.lang.String")) {
        		    logDebug("String")
        		    genericRecord.put(column.getName, rtemp.toString())
        	    } else {
        		    logDebug("Double")
        		    genericRecord.put(column.getName, classOf[java.lang.Double].cast(rtemp).doubleValue())
        	    }
        		isARecord = true
        	} else if (rtemp.isInstanceOf[java.lang.Byte]) {
        		logDebug("Byte")
        		genericRecord.put(
        			column.getName,
        			classOf[Byte].cast(rtemp).intValue())
        		isARecord = true
        	} else if (rtemp.isInstanceOf[java.lang.String]) {
        		logDebug("String found, column type is " + column.getType + " for name " + column.getName)
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
        		logDebug("Spark type " + rtemp.getClass())
        		logDebug("Kin instance type is type/name" + column.getType + "/" + column.getName)
        		genericRecord.put(
        			column.getName,
        			column.getType.cast(rtemp))
        		isARecord = true
        	}
        } 
        isARecord
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
