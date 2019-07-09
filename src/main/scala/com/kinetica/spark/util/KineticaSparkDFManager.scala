package com.kinetica.spark.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Iterator;

import scala.beans.BeanProperty;
import scala.collection.JavaConversions.asScalaBuffer;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.gpudb.BulkInserter;
import com.gpudb.ColumnProperty;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.GPUdbException;
import com.gpudb.GenericRecord;
import com.gpudb.Type;
import com.kinetica.spark.LoaderParams;
import com.typesafe.scalalogging.LazyLogging;

import java.nio.ByteBuffer;

import scala.collection.JavaConversions._;

object KineticaSparkDFManager extends LazyLogging {
    
    val MAX_DATE = 29379542399999L;
    val MIN_DATE = -30610224000000L;

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


    /**
     * Sets the type of the Kinetica table.
     *
     * @param lp LoaderParams  Contains all parameters
     */
    def setType(lp: LoaderParams): Unit = {
        try {
            logger.debug("Kinetica URL is " + lp.getKineticaURL)
            val gpudb: GPUdb = lp.getGpudb
            logger.debug(" Attempting Type.fromTable for table name " + lp.getTablename)
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
    def toBoolean: (Any) => Boolean = { case i: Boolean => i }
    def bool2int(b:Boolean) = if (b) 1 else 0

    /**
     * Maps over dataframe using either matching columns or chronological ordering
     * @param lp LoaderParams
     */
    def KineticaMapWriter(sc: SparkContext, lp: LoaderParams): Unit = {

        logger.debug("KineticaMapWriter")
        val typef: Type = myType
        val bkp: LoaderParams = lp
        logger.debug("Mapping Dataset columns to Kinetica")
        df.foreachPartition(new ForeachPartitionFunction[Row]() {
            def call(t: Iterator[Row]): Unit = {
                val kbl: KineticaBulkLoader = new KineticaBulkLoader(bkp)
                val bi: BulkInserter[GenericRecord] = kbl.GetBulkInserter()
                while (t.hasNext) {
                    
                    lp.totalRows.add(1)
                    val row: Row = t.next()
                    val genericRecord: GenericRecord = new GenericRecord(typef)
                    var i: Int = 0
                    for (column <- typef.getColumns) {
                        try {
                            var rtemp: Any = row.get({ i += 1; i - 1 })
                            if (lp.isMapToSchema) {
                                rtemp = row.getAs(column.getName)
                            }
                            if( rtemp != null ) { // not a nul value
                                if (!putInGenericRecord(genericRecord, rtemp, column)) {
                                    lp.failedConversion.add(1)
                                }
                            }
                        } catch {
                            case e: Exception => {
                                //e.printStackTrace()
                                lp.failedConversion.add(1);
                                logger.warn(s"Found non-matching column DS.column --> KineticaTable.column, moving on; Issue: '${e.getMessage()}'" );
                                throw e;
                            }
                        }
                    }
                    lp.convertedRows.add(1)
                    bi.insert(genericRecord)
                }
                try {
                    bi.flush();
                } catch {
                    case e: Exception => {
                        logger.error("Flush error", e)
                        e.printStackTrace()
                    }
                }
            }
        })
    }


    /**
     * Put a given row's given column value in the record compatible with Kinetica.
     */
    def putInGenericRecord(genericRecord : GenericRecord, rtemp : Any, column : Type.Column ) : Boolean = {
        
        //println(" Adding 1 record 1 field .........")
        val columnName = column.getName();
        val columnType = column.getType();

        
        var isARecord: Boolean = false
        if (rtemp != null) {
            logger.debug("Spark data type {} not null, column '{}'", rtemp.getClass(),  columnName)
            if (rtemp.isInstanceOf[java.lang.Long]) {
                logger.debug("Long")
                    genericRecord.put(columnName, rtemp)
                    isARecord = true
                    } else if (rtemp.isInstanceOf[java.lang.Integer]) {
                logger.debug("handling Integer")
                if (column.getType().toString().contains("java.lang.Integer")) {
                    genericRecord.put(columnName, rtemp)
                    isARecord = true
                } else if (column.getType().toString().contains("java.lang.Long")) {
                    genericRecord.put(columnName, toLong(rtemp))
                    isARecord = true
                } else if (column.getType().toString().contains("java.lang.String")) {
                    genericRecord.put(columnName, rtemp.toString())
                    isARecord = true
                } else if (column.getType().toString().contains("java.lang.Double")) {
                    genericRecord.put(columnName, toDouble(rtemp))
                    isARecord = true
                } else {
                    logger.debug("***** Kinetica column type is " + column.getType) 
                }
            } else if (rtemp.isInstanceOf[Timestamp]) {
                logger.debug("Timestamp instance")
                if (column.getType().toString().contains("java.lang.String")) {
                    logger.debug("Timestamp to date conversion")
                    if ( column.hasProperty( ColumnProperty.DATE ) ) {
                        logger.debug("Timestamp to date conversion")
                        val timelong = classOf[Timestamp].cast(rtemp).getTime 
                        val formatter = new java.text.SimpleDateFormat("YYYY-MM-dd")
                        val dateString = formatter.format(timelong)
                        logger.debug("Date string is " + dateString)
                        genericRecord.put(columnName, dateString)
                    } else {
                        // TODO - Handle datetime 
                        logger.info("Kin col properties " + column.getProperties)
                    }
                } else {
                    var sinceepoch = classOf[Timestamp].cast(rtemp).getTime
                    if( sinceepoch > MAX_DATE ) {
                        sinceepoch = MAX_DATE;
                    } else if( sinceepoch < MIN_DATE ) {
                        sinceepoch = MIN_DATE;
                    }           
                    logger.debug(" ################ " + sinceepoch)
                    genericRecord.put(columnName, sinceepoch)
                }
                isARecord = true
            } else if (rtemp.isInstanceOf[java.sql.Date]) {
                logger.debug("Date instance")
                genericRecord.put(columnName, rtemp.toString)
                isARecord = true
            } else if (rtemp.isInstanceOf[java.lang.Boolean]) {
                logger.debug("Boolean instance xxx " + toBoolean(rtemp))
                genericRecord.put(columnName, bool2int(toBoolean(rtemp)))
                isARecord = true
            } else if (rtemp.isInstanceOf[BigDecimal]) {
                logger.debug("BigDecimal")
                // The SQL decimal type can be mapped to a variety of Kinetica
                // types; so we need to check the column's type
                columnType match {
                    case q if (q == classOf[java.lang.Double]) => {
                        genericRecord.put( columnName, classOf[java.math.BigDecimal].cast(rtemp).doubleValue() );
                    }
                    case q if (q == classOf[java.lang.Float]) => {
                        genericRecord.put( columnName, classOf[java.math.BigDecimal].cast(rtemp).floatValue() );
                    }
                    case q if (q == classOf[java.lang.Integer]) => {
                        genericRecord.put( columnName, classOf[java.math.BigDecimal].cast(rtemp).intValue() );
                    }
                    case q if (q == classOf[java.lang.Long]) => {
                        genericRecord.put( columnName, classOf[java.math.BigDecimal].cast(rtemp).longValue() );
                    }
                    case q if (q == classOf[java.lang.String]) => {
                        genericRecord.put( columnName, rtemp.toString );
                    }
                }
                isARecord = true
            } else if (rtemp.isInstanceOf[java.lang.Short]) {
                logger.debug("Short")
                genericRecord.put(columnName, classOf[java.lang.Short].cast(rtemp).intValue())
                isARecord = true
            } else if (rtemp.isInstanceOf[java.lang.Float]) {
                logger.debug("Float")
                if (column.getType().toString().contains("java.lang.Float")) {
                    genericRecord.put(columnName, classOf[java.lang.Float].cast(rtemp).floatValue())
                    isARecord = true
                } else if (column.getType().toString().contains("java.lang.Double")) {
                    genericRecord.put(columnName, toDouble(rtemp))
                    isARecord = true
                } else if (column.getType().toString().contains("java.lang.String")) {
                    genericRecord.put(columnName, rtemp.toString())
                    isARecord = true
                } else {
                    logger.debug("**** Kinetica column type is " + column.getType) 
                }
            } else if (rtemp.isInstanceOf[java.lang.Double]) {
                logger.debug("Double")
                if (column.getType().toString().contains("java.lang.String")) {
                    genericRecord.put(columnName, rtemp.toString())
                } else if (column.getType().toString().contains("java.lang.Float")) {
                    genericRecord.put(columnName, classOf[java.lang.Double].cast(rtemp).floatValue())
                } else {
                    genericRecord.put(columnName, classOf[java.lang.Double].cast(rtemp).doubleValue())
                }
                isARecord = true
            } else if (rtemp.isInstanceOf[java.lang.Byte]) {
                logger.debug("Byte")
                genericRecord.put( columnName,
                                   classOf[java.lang.Byte].cast(rtemp).intValue());
                // classOf[Byte].cast(rtemp).intValue())
                isARecord = true
            } else if (rtemp.isInstanceOf[java.lang.String]) {
                logger.debug("String found, column type is " + column.getType)
                // This is the path most travelled....
                if (column.getType().toString().contains("java.lang.Double")) {
                    genericRecord.put(columnName, rtemp.toString().toDouble)
                } else if (column.getType().toString().contains("java.lang.Float")) {
                    genericRecord.put(columnName, rtemp.toString().toFloat)
                } else if (column.getType().toString().contains("java.lang.Integer")) {
                    genericRecord.put(columnName, rtemp.toString().toInt)
                } else if (column.getType().toString().contains("java.lang.Long")) {
                    if ( column.hasProperty( ColumnProperty.TIMESTAMP ) ) {
                        logger.debug("String to timestamp conversion")
                        // The full timestamp format has optional parts
                        val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd[[ ]['T']HH:mm[:ss][.SSS]]");
                        // Get the number of milliseconds since the epoch
                        var timestamp : Long = 0;
                        try {
                            // Try parsing a full timestamp with the date and the time
                            timestamp = java.time.LocalDateTime.parse( rtemp.toString(),
                                                                       formatter )
                                .atZone( java.time.ZoneId.systemDefault() )
                                .toInstant()
                                .toEpochMilli();
                        } catch {
                            case e: java.time.format.DateTimeParseException => {
                                // Try parsing just the date
                                timestamp = java.time.LocalDate.parse( rtemp.toString(),
                                                                       formatter )
                                .atStartOfDay()
                                .atZone( java.time.ZoneId.systemDefault() )
                                .toInstant()
                                .toEpochMilli();
                            }
                        }
                        genericRecord.put( columnName, timestamp );
                    } else {
                       logger.debug("String to long conversion");
                       genericRecord.put(columnName, rtemp.toString().toLong);
                    }
                } else {
                    genericRecord.put(columnName, rtemp.toString())
                }
                isARecord = true
            } else if (rtemp.isInstanceOf[Array[Byte]]) {
                logger.debug("Byte array found, column type is " + column.getType)
                logger.debug("Byte array lnegth = " + rtemp.asInstanceOf[Array[Byte]].length)
                genericRecord.put(columnName, ByteBuffer.wrap(rtemp.asInstanceOf[Array[Byte]]))
                isARecord = true
            } else {
        	 
                logger.debug("Spark type {} Kin instance type is {} ", rtemp.getClass(), column.getType)
                genericRecord.put(
                                  columnName,
                                  //column.getType.cast(rtemp))
                                  rtemp.toString())
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
