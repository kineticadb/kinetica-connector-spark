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

class KineticaSparkDFManager extends LazyLogging
    with Serializable {
    
    val MAX_DATE = 29379542399999L;
    val MIN_DATE = -30610224000000L;

    @BeanProperty
    @transient
    var df: DataFrame = null;

    @BeanProperty
    var timeZone: java.util.TimeZone = null;

    @BeanProperty
    var zoneID: java.time.ZoneId = null;

    @BeanProperty
    var loaderParams : LoaderParams = null;
    
    var myType: Type = null;


    /// Constructor
    def this( lp: LoaderParams ) = {
        this();

        // Save the configuration parameters
        this.loaderParams = lp;

        // Set the type
        setType( lp );

        // Set the time zone to be used for any datetime ingestion
        timeZone = lp.getTimeZone;
        zoneID   = timeZone.toZoneId();
    }
        

        
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

        // Set the parameters to be used by other functions
        loaderParams = lp;
        
        logger.debug("Mapping Dataset columns to Kinetica")
        df.foreachPartition(new ForeachPartitionFunction[Row]() {
            def call(t: Iterator[Row]): Unit = {
                val kbl: KineticaBulkLoader = new KineticaBulkLoader(bkp)
                val bi: BulkInserter[GenericRecord] = kbl.GetBulkInserter()

                var numRecordsProcessed = 0;
                var numRecordsFailed = 0;
                
                while (t.hasNext) {

                    numRecordsProcessed += 1;
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
                                numRecordsFailed += 1;
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
                        // Update the count of successful conversions to reflect
                        // this failure
                        val totalConvertedRows = lp.convertedRows.value;
                        lp.convertedRows.reset();
                        lp.convertedRows.add( totalConvertedRows - numRecordsProcessed );

                        // Update the failed conversions
                        lp.failedConversion.add( numRecordsProcessed - numRecordsFailed );
                        
                        if ( lp.failOnError ) {
                            logger.error("Flush error", e);
                            throw e;
                        } else {
                            logger.error( s"Failed to ingest a batch of records; issue: '${e.getMessage()}" );
                            logger.debug( s"Failed to ingest a batch of records; stacktrace for debugging: ", e );
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
        val columnName     = column.getName();
        val columnType     = column.getColumnType();
        val columnBaseType = column.getColumnBaseType();
        val columnTypeStr  = column.getType().toString();

        logger.debug(s"Column name: '${columnName}'; Kinetica type: '${columnType}'; Kinetica base type: '${columnBaseType}'");
        
        var isColumnValid: Boolean = false
        if (rtemp != null) {
            // logger.debug("Spark data type {} not null, column '{}'", rtemp.getClass(),  columnName)
            if (rtemp.isInstanceOf[java.lang.Long]) {
                // Spark data type is long
                logger.debug(s"Type: long; column name '$columnName'");
                columnBaseType match {
                    case Type.Column.ColumnBaseType.FLOAT => {
                        logger.debug("Column type float");
                        genericRecord.put(columnName, classOf[java.lang.Long].cast(rtemp).floatValue());
                        isColumnValid = true;
                    }
                    case _ => {
                        genericRecord.put(columnName, rtemp)
                        isColumnValid = true
                    }
                }
            } else if (rtemp.isInstanceOf[java.lang.Integer]) {
                // Spark data type is integer
                logger.debug(s"Spark type: Integer; Kinetica base type: '${columnBaseType}'");
                // Check the column's base/avro type
                columnBaseType match {
                    case Type.Column.ColumnBaseType.INTEGER => {
                        genericRecord.put(columnName, rtemp);
                        isColumnValid = true;
                    }
                    case Type.Column.ColumnBaseType.LONG => {
                        genericRecord.put(columnName, toLong(rtemp));
                        isColumnValid = true;
                    }
                    case Type.Column.ColumnBaseType.STRING => {
                        genericRecord.put(columnName, rtemp.toString());
                        isColumnValid = true;
                    }
                    case Type.Column.ColumnBaseType.DOUBLE => {
                        genericRecord.put(columnName, toDouble(rtemp));
                        isColumnValid = true;
                    }
                    case Type.Column.ColumnBaseType.FLOAT => {
                        logger.debug("Column type float");
                        genericRecord.put(columnName, classOf[java.lang.Integer].cast(rtemp).floatValue());
                        isColumnValid = true;
                    }
                    case _ => {
                        logger.debug(s"Unknown column type: ${columnTypeStr}");
                    }
                }
            } else if (rtemp.isInstanceOf[Timestamp]) {
                // Spark data type is timestamp
                logger.debug(s"Spark type: Timestamp; Kinetica base type: '${columnBaseType}'; Kinetica type: '${columnType}'");
                // Check againt base type
                columnBaseType match {
                    case Type.Column.ColumnBaseType.STRING => {
                        // Put in the value as a string
                        var stringValue: String = rtemp.toString();
                        genericRecord.putDateTime( columnName, stringValue, timeZone );
                    }
                    case _ => {
                        var sinceepoch = classOf[Timestamp].cast(rtemp).getTime
                        if( sinceepoch > MAX_DATE ) {
                            sinceepoch = MAX_DATE;
                        } else if( sinceepoch < MIN_DATE ) {
                            sinceepoch = MIN_DATE;
                        }           
                        logger.debug(" ################ " + sinceepoch)
                        genericRecord.put(columnName, sinceepoch)
                    }
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
                logger.debug(s"Spark type: BigDecimal; Kinetica base type: '${columnBaseType}'; Kinetica type: '${columnType}'");
                // The SQL decimal type can be mapped to a variety of Kinetica
                // types; so we need to check the column's BASE type
                columnBaseType match {
                    case Type.Column.ColumnBaseType.DOUBLE => {
                        genericRecord.put( columnName, classOf[java.math.BigDecimal].cast(rtemp).doubleValue() );
                    }
                    case Type.Column.ColumnBaseType.FLOAT => {
                        genericRecord.put( columnName, classOf[java.math.BigDecimal].cast(rtemp).floatValue() );
                    }
                    case Type.Column.ColumnBaseType.INTEGER => {
                        genericRecord.put( columnName, classOf[java.math.BigDecimal].cast(rtemp).intValue() );
                    }
                    case Type.Column.ColumnBaseType.LONG => {
                        genericRecord.put( columnName, classOf[java.math.BigDecimal].cast(rtemp).longValue() );
                    }
                    case Type.Column.ColumnBaseType.STRING => {
                        genericRecord.put( columnName, rtemp.toString );
                    }
                    case _ => {
                        logger.debug(s"Unhandled type '$columnTypeStr' for column '$columnName' for dataframe type 'BigDecimal'");
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
                logger.debug(s"Spark type: Float; Kinetica base type: '${columnBaseType}'; Kinetica type: '${columnType}'");
                // Check against the base type
                columnBaseType match {
                    case Type.Column.ColumnBaseType.FLOAT => {
                        genericRecord.put(columnName, classOf[java.lang.Float].cast(rtemp).floatValue());
                        isColumnValid = true;
                    }
                    case Type.Column.ColumnBaseType.DOUBLE => {
                        genericRecord.put(columnName, toDouble(rtemp));
                        isColumnValid = true;
                    }
                    case Type.Column.ColumnBaseType.STRING => {
                        genericRecord.put(columnName, rtemp.toString());
                        isColumnValid = true;
                    }
                    case _ => {
                        logger.debug(s"Unhandled Kinetica column type: '${columnType}'") ;
                    }
                }
            } else if (rtemp.isInstanceOf[java.lang.Double]) {
                // Spark data type is double
                logger.debug(s"Spark type: Double; Kinetica base type: '${columnBaseType}'; Kinetica type: '${columnType}'");
                columnBaseType match {
                    case Type.Column.ColumnBaseType.STRING => {
                        genericRecord.put(columnName, rtemp.toString());
                    }
                    case Type.Column.ColumnBaseType.FLOAT => {
                        genericRecord.put(columnName, classOf[java.lang.Double].cast(rtemp).floatValue());
                    }
                    case _ => {
                        genericRecord.put(columnName, classOf[java.lang.Double].cast(rtemp).doubleValue());
                    }
                }
                isColumnValid = true;
            } else if (rtemp.isInstanceOf[java.lang.Byte]) {
                // Spark data type is byte
                logger.debug("Byte")
                genericRecord.put( columnName,
                                   classOf[java.lang.Byte].cast(rtemp).intValue());
                // classOf[Byte].cast(rtemp).intValue())
                isColumnValid = true
            } else if (rtemp.isInstanceOf[java.lang.String]) {
                // Spark data type is string
                // This is the path most travelled....
                logger.debug(s"Spark type: String; Kinetica base type: '${columnBaseType}'; Kinetica type: '${columnType}'");
                columnBaseType match {
                    case Type.Column.ColumnBaseType.DOUBLE => {
                        genericRecord.put(columnName, rtemp.toString().toDouble);
                    }
                    case Type.Column.ColumnBaseType.FLOAT => {
                        genericRecord.put(columnName, rtemp.toString().toFloat);
                    }
                    case Type.Column.ColumnBaseType.INTEGER => {
                        genericRecord.put(columnName, rtemp.toString().toInt);
                    }
                    case Type.Column.ColumnBaseType.LONG => {
                        // Need to handle different sub-types of long
                        columnType match {
                            case Type.Column.ColumnType.TIMESTAMP => {
                                logger.debug("String to timestamp conversion");
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
                            }
                            case Type.Column.ColumnType.LONG => {
                                logger.debug("String to long conversion");
                                genericRecord.put(columnName, rtemp.toString().toLong);
                            }
                            case _ => {
                                logger.debug(s"Unhandled Kinetica type: '${columnType}'");
                            }
                        }
                    }
                    case Type.Column.ColumnBaseType.STRING => {
                        logger.debug(s"Base type string, column type is ${columnType}; column name '$columnName'");
                        // Need to handle different sub-types of string
                        columnType match {
                            case Type.Column.ColumnType.CHAR1 => {
                                val value = TextUtils.truncateToSize( loaderParams.truncateToSize,
                                                                      rtemp.toString(), 1 );
                                genericRecord.put( columnName, value );
                            }
                            case Type.Column.ColumnType.CHAR2 => {
                                val value = TextUtils.truncateToSize( loaderParams.truncateToSize,
                                                                      rtemp.toString(), 2 );
                                genericRecord.put( columnName, value );
                            }
                            case Type.Column.ColumnType.CHAR4 => {
                                val value = TextUtils.truncateToSize( loaderParams.truncateToSize,
                                                                      rtemp.toString(), 4 );
                                genericRecord.put( columnName, value );
                            }
                            case Type.Column.ColumnType.CHAR8 => {
                                val value = TextUtils.truncateToSize( loaderParams.truncateToSize,
                                                                      rtemp.toString(), 8 );
                                genericRecord.put( columnName, value );
                            }
                            case Type.Column.ColumnType.CHAR16 => {
                                val value = TextUtils.truncateToSize( loaderParams.truncateToSize,
                                                                      rtemp.toString(), 16 );
                                genericRecord.put( columnName, value );
                            }
                            case Type.Column.ColumnType.CHAR32 => {
                                val value = TextUtils.truncateToSize( loaderParams.truncateToSize,
                                                                      rtemp.toString(), 32 );
                                genericRecord.put( columnName, value );
                            }
                            case Type.Column.ColumnType.CHAR64 => {
                                val value = TextUtils.truncateToSize( loaderParams.truncateToSize,
                                                                      rtemp.toString(), 64 );
                                genericRecord.put( columnName, value );
                            }
                            case Type.Column.ColumnType.CHAR128 => {
                                val value = TextUtils.truncateToSize( loaderParams.truncateToSize,
                                                                      rtemp.toString(), 128 );
                                genericRecord.put( columnName, value );
                            }
                            case Type.Column.ColumnType.CHAR256 => {
                                val value = TextUtils.truncateToSize( loaderParams.truncateToSize,
                                                                      rtemp.toString(), 256 );
                                genericRecord.put( columnName, value );
                            }
                            case Type.Column.ColumnType.DATE     |
                                 Type.Column.ColumnType.DATETIME |
                                 Type.Column.ColumnType.TIME => {
                                // Need to specify the timezone
                                genericRecord.putDateTime( columnName, rtemp.toString(), timeZone );
                            }
                            case _ => {
                                genericRecord.put( columnName, rtemp.toString() );
                            }
                        }   // end match column type (sub-type)
                    }   // end base type string
                    case Type.Column.ColumnBaseType.BYTES => {
                        genericRecord.put( columnName, ByteBuffer.wrap( rtemp.toString().getBytes() ) );
                    }
                }   // end match base type
                isColumnValid = true;
            } else if (rtemp.isInstanceOf[Array[Byte]]) {
                // Spark data type is byte array
                logger.debug(s"Byte array found, column type is ${columnType}");
                logger.debug("Byte array length = " + rtemp.asInstanceOf[Array[Byte]].length);
                genericRecord.put(columnName, ByteBuffer.wrap(rtemp.asInstanceOf[Array[Byte]]));
                isColumnValid = true;
            } else {
        	 
                logger.debug("Spark type {} Kin instance type is {} ", rtemp.getClass(), column.getType);
                genericRecord.put(
                                  columnName,
                                  //column.getType.cast(rtemp))
                                  rtemp.toString());
                isColumnValid = true;
            }
        } 

        isColumnValid;
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
