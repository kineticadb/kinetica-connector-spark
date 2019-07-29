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
import java.time.ZoneId;

import scala.collection.JavaConversions._;

object KineticaSparkDFManager extends LazyLogging {
    
    val MAX_DATE = 29379542399999L;
    val MIN_DATE = -30610224000000L;

    @BeanProperty
    var df: DataFrame = null;

    @BeanProperty
    var timeZone: java.util.TimeZone = null;

    @BeanProperty
    var zoneID: java.time.ZoneId = null;
    
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

        // Set the time zone to be used for any datetime ingestion
        timeZone = lp.getTimeZone;
        zoneID   = timeZone.toZoneId();
        
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

                    var isRecordGood = true;
                    
                    // Convert all the column values and put in the record
                    for (column <- typef.getColumns) {
                        try {
                            var rtemp: Any = row.get({ i += 1; i - 1 })
                            if (lp.isMapToSchema) {
                                rtemp = row.getAs(column.getName)
                            }
                            if( rtemp != null ) { // not a nul value
                                if (!putInGenericRecord(genericRecord, rtemp, column)) {
                                    lp.failedConversion.add(1);
                                    isRecordGood = false;
                                }
                            }
                        } catch {
                            case e: Exception => {
                                //e.printStackTrace()
                                lp.failedConversion.add(1);
                                isRecordGood = false;
                                logger.warn(s"Found non-matching column DS.column --> KineticaTable.column, moving on; Issue: '${e.getMessage()}'" );
                                logger.debug(s"Found non-matching column DS.column --> KineticaTable.column, moving on; Issue: ", e );
                                if ( lp.failOnError ) {
                                    // Throw exception only for fail-fast mode
                                    throw e;
                                }
                            }
                        }
                    }

                    if ( isRecordGood ) {
                        lp.convertedRows.add( 1 );
                        bi.insert( genericRecord );
                    }
                }
                try {
                    bi.flush();
                } catch {
                    case e: Exception => {
                        if ( lp.failOnError ) {
                            logger.error("Flush error", e);
                            throw e;
                        } else {
                            logger.error( s"Failed to ingest a batch of records; issue: '${e.getMessage()}" );
                        }
                    }
                }
            }
        })
    }


    /**
     * Put a given row's given column value in the record compatible with Kinetica.
     *
     * Returns if the column value was successfully converted and placed in the record.
     */
    def putInGenericRecord(genericRecord : GenericRecord, rtemp : Any, column : Type.Column ) : Boolean = {
        
        //println(" Adding 1 record 1 field .........")
        val columnName    = column.getName();
        val columnType    = column.getType();
        val columnTypeStr = columnType.toString();

        
        var isColumnValid: Boolean = false
        if (rtemp != null) {
            // logger.debug("Spark data type {} not null, column '{}'", rtemp.getClass(),  columnName)
            if (rtemp.isInstanceOf[java.lang.Long]) {
                // Spark data type is long
                logger.debug("Long");
                if (columnTypeStr.contains("java.lang.Float")) {
                    logger.debug("Column type float");
                    genericRecord.put(columnName, classOf[java.lang.Long].cast(rtemp).floatValue());
                    isColumnValid = true;
                } else {
                    genericRecord.put(columnName, rtemp)
                    isColumnValid = true
                }
            } else if (rtemp.isInstanceOf[java.lang.Integer]) {
                // Spark data type is integer
                // logger.debug("handling Integer")
                if (columnTypeStr.contains("java.lang.Integer")) {
                    genericRecord.put(columnName, rtemp)
                    isColumnValid = true
                } else if (columnTypeStr.contains("java.lang.Long")) {
                    genericRecord.put(columnName, toLong(rtemp))
                    isColumnValid = true
                } else if (columnTypeStr.contains("java.lang.String")) {
                    genericRecord.put(columnName, rtemp.toString())
                    isColumnValid = true
                } else if (columnTypeStr.contains("java.lang.Double")) {
                    genericRecord.put(columnName, toDouble(rtemp))
                    isColumnValid = true
                } else if (columnTypeStr.contains("java.lang.Float")) {
                    logger.debug("Column type float");
                    genericRecord.put(columnName, classOf[java.lang.Integer].cast(rtemp).floatValue());
                    isColumnValid = true;
                } else {
                    logger.debug("Unknown column type: " + column.getType) 
                }
            } else if (rtemp.isInstanceOf[Timestamp]) {
                // Spark data type is timestamp
                if (columnTypeStr.contains("java.lang.String")) {
                    // Put in the value as a string
                    var stringValue: String = rtemp.toString();
                    genericRecord.putDateTime( columnName, stringValue, timeZone );
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
                isColumnValid = true
            } else if (rtemp.isInstanceOf[java.sql.Date]) {
                // Spark data type is date
                logger.debug("Date instance")
                genericRecord.put(columnName, rtemp.toString)
                isColumnValid = true
            } else if (rtemp.isInstanceOf[java.lang.Boolean]) {
                // Spark data type is bool
                logger.debug("Boolean instance xxx " + toBoolean(rtemp))
                genericRecord.put(columnName, bool2int(toBoolean(rtemp)))
                isColumnValid = true
            } else if (rtemp.isInstanceOf[BigDecimal]) {
                // Spark data type is BigDecimal
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
                isColumnValid = true
            } else if (rtemp.isInstanceOf[java.lang.Short]) {
                // Spark data type is short
                logger.debug("Short")
                genericRecord.put(columnName, classOf[java.lang.Short].cast(rtemp).intValue())
                isColumnValid = true
            } else if (rtemp.isInstanceOf[java.lang.Float]) {
                // Spark data type is float
                logger.debug("Float")
                if (columnTypeStr.contains("java.lang.Float")) {
                    genericRecord.put(columnName, classOf[java.lang.Float].cast(rtemp).floatValue())
                    isColumnValid = true
                } else if (columnTypeStr.contains("java.lang.Double")) {
                    genericRecord.put(columnName, toDouble(rtemp))
                    isColumnValid = true
                } else if (columnTypeStr.contains("java.lang.String")) {
                    genericRecord.put(columnName, rtemp.toString())
                    isColumnValid = true
                } else {
                    logger.debug("**** Kinetica column type is " + column.getType) 
                }
            } else if (rtemp.isInstanceOf[java.lang.Double]) {
                // Spark data type is double
                logger.debug("Double")
                if (columnTypeStr.contains("java.lang.String")) {
                    genericRecord.put(columnName, rtemp.toString())
                } else if (columnTypeStr.contains("java.lang.Float")) {
                    genericRecord.put(columnName, classOf[java.lang.Double].cast(rtemp).floatValue())
                } else {
                    genericRecord.put(columnName, classOf[java.lang.Double].cast(rtemp).doubleValue())
                }
                isColumnValid = true
            } else if (rtemp.isInstanceOf[java.lang.Byte]) {
                // Spark data type is byte
                logger.debug("Byte")
                genericRecord.put( columnName,
                                   classOf[java.lang.Byte].cast(rtemp).intValue());
                // classOf[Byte].cast(rtemp).intValue())
                isColumnValid = true
            } else if (rtemp.isInstanceOf[java.lang.String]) {
                // Spark data type is string
                logger.debug("String found, column type is " + column.getType)
                // This is the path most travelled....
                if (columnTypeStr.contains("java.lang.Double")) {
                    genericRecord.put(columnName, rtemp.toString().toDouble)
                } else if (columnTypeStr.contains("java.lang.Float")) {
                    genericRecord.put(columnName, rtemp.toString().toFloat)
                } else if (columnTypeStr.contains("java.lang.Integer")) {
                    genericRecord.put(columnName, rtemp.toString().toInt)
                } else if (columnTypeStr.contains("java.lang.Long")) {
                    if ( column.hasProperty( ColumnProperty.TIMESTAMP ) ) {
                        logger.debug("String to timestamp conversion")
                        // The full timestamp format has optional parts
                        val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy[-][/][.]MM[-][/][.]dd[[ ]['T']HH:mm[:ss][.SSS][ ][XXX][Z][z][VV][x]]");
                        // Get the number of milliseconds since the epoch
                        var timestamp : Long = 0;
                        try {
                            // Try parsing a full timestamp with the date and the time
                            timestamp = java.time.LocalDateTime.parse( rtemp.toString(),
                                                                       formatter )
                                .atZone( zoneID )
                                .toInstant()
                                .toEpochMilli();
                        } catch {
                            case e: java.time.format.DateTimeParseException => {
                                // Try parsing just the date
                                timestamp = java.time.LocalDate.parse( rtemp.toString(),
                                                                       formatter )
                                .atStartOfDay()
                                .atZone( zoneID )
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
                    // Parse date, time, datetime values for non-Kinetica formats
                    // as well as Kinetica formats; other types get inserted without
                    // any processing
                    genericRecord.putDateTime( columnName, rtemp.toString(), timeZone );
                }
                isColumnValid = true
            } else if (rtemp.isInstanceOf[Array[Byte]]) {
                // Spark data type is byte array
                logger.debug("Byte array found, column type is " + column.getType)
                logger.debug("Byte array lnegth = " + rtemp.asInstanceOf[Array[Byte]].length)
                genericRecord.put(columnName, ByteBuffer.wrap(rtemp.asInstanceOf[Array[Byte]]))
                isColumnValid = true
            } else {
        	 
                logger.debug("Spark type {} Kin instance type is {} ", rtemp.getClass(), column.getType)
                genericRecord.put(
                                  columnName,
                                  //column.getType.cast(rtemp))
                                  rtemp.toString())
                isColumnValid = true
            }
        } 
        isColumnValid
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
