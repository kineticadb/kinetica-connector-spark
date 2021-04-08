package com.kinetica.spark.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
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

class KineticaSparkDFManager extends LazyLogging
    with Serializable {

    val MAX_DATE = 29379542399999L;
    val MIN_DATE = -30610224000000L;

    // Regex for identifying long values (positive or negative)
    val LONG_REGEX = "(-)*(\\d+)";

    // The full timestamp format with optional parts.  Note that the member has
    // to be transient and lazy so that each executor creates its own instance.
    // This is needed since the Java class is not serializable.
    @transient
    lazy val TIMESTAMP_FORMATTER = java.time.format.DateTimeFormatter.ofPattern("yyyy[-][/][.]MM[-][/][.]dd[[ ]['T']HH:mm[:ss][.SSSSSSSSS][.SSSSSS][.SSS][ ][XXX][Z][z][VV][x]]");

    @BeanProperty
    @transient
    var df: DataFrame = null;

    @BeanProperty
    var timeZone: java.util.TimeZone = null;

    @BeanProperty
    var zoneID: ZoneId = null;

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
     * Returns LocalDateTime calculated from long value provided as
     * milliseconds since epoch in UTC timezone (no offset)
     * @return java.time.LocalDateTime
     */
    def convertTimestampToDatetime(sinceEpochMilli: Long): LocalDateTime = {
        // Converts long ts into LocalDateTime object, respecting the
        // MIN_DATE and MAX_DATE boundaries.
        LocalDateTime.ofInstant(
            Instant.ofEpochMilli(
                getValidTimestamp(sinceEpochMilli)
            ),
            ZoneId.of("UTC")
        )
    }

    /**
     * Returns Long timestamp value in
     * milliseconds since epoch in UTC timezone (no offset)
     * that can be safely saved in Kinetica DB
     * @return Long
     */
    def getValidTimestamp(sinceEpochMilli: Long): Long = {
        // Kinetica can store only dates within range
        // [1000-01-01T00:00 - 2900-12-31T23:59:59.999]
        // Dates outside the range are forced to boundary values
        if( sinceEpochMilli > MAX_DATE ) {
            return (MAX_DATE)
        } else if( sinceEpochMilli < MIN_DATE ) {
            return(MIN_DATE)
        } else {
            return (sinceEpochMilli)
        }
    }

    /**
     * Parses String literal for Datetime/Date/Time using the
     * provided formatter to milliseconds since epoch
     * in UTC timezone (no offset)
     * @return Long
     */
    def parseDatetimeToTimestamp(value: String, formatter: java.time.format.DateTimeFormatter): Long = {
        // Converts any string literal of Date / Time / Time with timezone / DateTime or
        // DateTime with timeZone formats to the number of milliseconds since the epoch
        var sinceepoch : Long = 0;

        try {
            // Try parsing a full timestamp with date, time and timezone
            sinceepoch = getValidTimestamp(
                ZonedDateTime.parse( value, formatter )
                    .toLocalDateTime()
                    .atZone( ZoneId.of("UTC") )
                    .toInstant()
                    .toEpochMilli()
            );

        } catch {
            case e: java.time.format.DateTimeParseException => {
                try {
                    // Try parsing a full local timestamp with date and time no timezone
                    sinceepoch = getValidTimestamp(
                        LocalDateTime.parse( value, formatter )
                            .atZone(  ZoneId.of("UTC")  )
                            .toInstant()
                            .toEpochMilli()
                    );
                } catch {
                    case e: java.time.format.DateTimeParseException => {
                        // Try parsing just the date
                        try {
                            sinceepoch = getValidTimestamp(
                                LocalDate.parse( value, formatter )
                                    .atStartOfDay()
                                    .toInstant(  ZoneId.of("UTC").getRules().getOffset(Instant.now()))
                                    .toEpochMilli()
                            );
                        } catch {
                            case e: java.time.format.DateTimeParseException => {
                                // Try parsing offset time to nanos of day
                                try {
                                    sinceepoch = OffsetTime.parse(value)
                                        .getLong(ChronoField.NANO_OF_DAY);
                                } catch {
                                    case e: java.time.format.DateTimeParseException => {
                                        // Parse local time to nanos of day
                                        // and don't intercept error in case it fails
                                        sinceepoch = LocalTime.parse(value)
                                            .getLong(ChronoField.NANO_OF_DAY)
                                    }
                                }

                            }
                        }
                    }
                }
            }
        }
        return (sinceepoch)
    }


    /**
     * Parses String literal for Datetime/Date/Time using the
     * provided formatter to ZonedDateTime in UTC timezone (no offset)
     * @return java.time.ZonedDateTime
     */
    def convertDateStringToLocalDateTimeUTC(value: String, formatter: java.time.format.DateTimeFormatter
    ): java.time.ZonedDateTime = {
        // Converts any string literal of Date / Time / Time with timezone / DateTime or
        // DateTime with timeZone formats to ZonedDateTime in "UTC" time zone.
        // Global Offset is the configured `zoneID` offset that the date literal should be converted to
        val globalZoneOffset: java.time.ZoneOffset = zoneID.getRules().getOffset(Instant.now());
        // If the string literal represents a zoned datetime,
        // the converted value has to take into account two different timezones:
        // one from global settings `zoneID` and one parsed from DateTime literal
        try {
            // Try parsing a full timestamp with date, time and timezone
            val parsed = ZonedDateTime.parse( value, formatter );
            // get timezone offset in seconds
            val offset = parsed.get(java.time.temporal.ChronoField.OFFSET_SECONDS);
            // modify the date with extracted offset because
            // offset is already calculated against UTC
            parsed
                // remove offset detected in ZonedDateTime literal
                .minusSeconds(offset)
                // add Global offset
                .plusSeconds(globalZoneOffset.getTotalSeconds())
                .toLocalDateTime()
                .atZone( ZoneId.of("UTC") );
        } catch {
            case e: java.time.format.DateTimeParseException => {
                try {
                    // Try parsing a full local timestamp with date and time (no timezone)
                    LocalDateTime.parse( value, formatter )
                        .atZone( ZoneId.of("UTC") );
                } catch {
                    case e: java.time.format.DateTimeParseException => {
                        // Try parsing just the date
                        try {
                            LocalDate.parse( value, formatter )
                                .atStartOfDay()
                                .atZone( ZoneId.of("UTC") );
                        } catch {
                            case e: java.time.format.DateTimeParseException => {
                                // Try parsing offset time and add date placeholder
                                try {
                                    val parsed = OffsetTime.parse(value);
                                    val offset = parsed.getOffset().getTotalSeconds();
                                    parsed
                                        // remove offset detected in OffsetTime literal
                                        .minusSeconds(offset)
                                        // add Global offset
                                        .plusSeconds(globalZoneOffset.getTotalSeconds())
                                        .toLocalTime()
                                        .atDate(LocalDate.now())
                                        .atZone( ZoneId.of("UTC") );
                                } catch {
                                    case e: java.time.format.DateTimeParseException => {
                                        // Parse local time and add date placeholder
                                        // and don't intercept error in case it fails
                                        LocalTime.parse(value)
                                            .atDate(LocalDate.now())
                                            .atZone( ZoneId.of("UTC") );
                                    }
                                }

                            }
                        }
                    }
                }
            }
        }
    }

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
                            if (!putInGenericRecord(genericRecord, rtemp, column)) {
                                if ( rtemp == null ) {
                                    logger.debug(s"Failed to set null value for column ${column.getName}");
                                } else {
                                    logger.debug(s"Failed to convert value ${rtemp} of type ${rtemp.getClass()} for column ${column.getName}");
                                }
                                isRecordGood = false;

                                // Throw exception only for the fail-fast mode
                                if ( lp.failOnError ) {
                                    val message = s"Failed to set value for column ${column.getName}";
                                    logger.error( message );
                                    throw new Exception( message );
                                }
                            }
                        } catch {
                            case e: Exception => {
                                //e.printStackTrace()
                                isRecordGood = false;
                                val message = s"Failed to set value for column ${column.getName}";
                                logger.warn( message );
                                logger.debug(s"${message}; reason: ", e);
                                if ( lp.failOnError ) {
                                    // Throw exception only for fail-fast mode
                                    logger.error(s"${message}; reason: ", e);
                                    // Since we'll throw, we need to increment the failed record
                                    // tally here (otherwise, it will be done in the else block
                                    // after this for loop.
                                    numRecordsFailed += 1;
                                    lp.failedConversion.add( 1 );
                                    throw e;
                                }
                            }
                        }
                    }  // end looping over columns

                    if ( isRecordGood ) {
                        lp.convertedRows.add( 1 );
                        bi.insert( genericRecord );
                    } else {
                        // Keep track of how many records will not be inserted
                        numRecordsFailed += 1;
                        lp.failedConversion.add(1);
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

        var isColumnValid: Boolean = true
        if ( rtemp == null ) {
            if ( !column.isNullable() ) {
                // We got a null value for a non-nullable column!
                logger.debug(s"Column '$columnName' is not nullable, but got a null value!");
                isColumnValid = false;
            } else {
                // It's ok to set a null value for a nullable column
                genericRecord.put( columnName, null );
            }
        } else {
            // logger.debug("Spark data type {} not null, column '{}'", rtemp.getClass(),  columnName)
            if (rtemp.isInstanceOf[java.lang.Long]) {
                // Spark data type is long
                logger.debug(s"Type: long; column name '$columnName'");
                columnBaseType match {
                    case Type.Column.ColumnBaseType.FLOAT => {
                        logger.debug("Column type float");
                        genericRecord.put(columnName, classOf[java.lang.Long].cast(rtemp).floatValue());
                    }
                    case Type.Column.ColumnBaseType.STRING => {
                        logger.debug(s"Kinetica base type String");
                        if (columnType == Type.Column.ColumnType.DATETIME) {
                            logger.debug("Column type DateTime");
                            try {
                                val result = convertTimestampToDatetime(classOf[java.lang.Long].cast(rtemp))
                                genericRecord.putDateTime( columnName,  result.toString(), timeZone );
                            } catch {
                                case e: Exception => {
                                    isColumnValid = false;
                                }
                            }
                        } else if (columnType == Type.Column.ColumnType.DATE) {
                            logger.debug("Column type Date");
                            try {
                                val result = convertTimestampToDatetime(classOf[java.lang.Long].cast(rtemp)).toLocalDate();
                                genericRecord.putDateTime( columnName,  result.toString(), timeZone );
                            } catch {
                                case e: Exception => {
                                    isColumnValid = false;
                                }
                            }
                        } else if (columnType == Type.Column.ColumnType.TIME) {
                            logger.debug("Column type Time");
                            val result = LocalTime.ofNanoOfDay(classOf[java.lang.Long].cast(rtemp));
                            genericRecord.putDateTime( columnName,  result.toString(), timeZone );
                            //isColumnValid = false;
                            //logger.warn(s"Can't convert column '${columnName}' from type java.lang.Long to type '${columnType}'.");

                        } else {
                            logger.debug("Value type Long to common String");
                            genericRecord.put( columnName,  classOf[java.lang.Long].cast(rtemp).toString() );
                        }
                    }
                    case _ => {
                        logger.debug("Just putting in a long");
                        genericRecord.put(columnName, rtemp)
                    }
                }
            } else if (rtemp.isInstanceOf[java.lang.Integer]) {
                // Spark data type is integer
                logger.debug(s"Spark type: Integer; Kinetica base type: '${columnBaseType}'");
                // Check the column's base/avro type
                columnBaseType match {
                    case Type.Column.ColumnBaseType.INTEGER => {
                        genericRecord.put(columnName, rtemp);
                    }
                    case Type.Column.ColumnBaseType.LONG => {
                        genericRecord.put(columnName, classOf[java.lang.Integer].cast(rtemp).longValue());
                    }
                    case Type.Column.ColumnBaseType.STRING => {
                        genericRecord.put(columnName, rtemp.toString());
                    }
                    case Type.Column.ColumnBaseType.DOUBLE => {
                        genericRecord.put(columnName, toDouble(rtemp));
                    }
                    case Type.Column.ColumnBaseType.FLOAT => {
                        logger.debug("Column type float");
                        genericRecord.put(columnName, classOf[java.lang.Integer].cast(rtemp).floatValue());
                    }
                    case _ => {
                        logger.debug(s"Unknown column type: ${columnTypeStr}");
                        isColumnValid = false;
                    }
                }
            } else if (rtemp.isInstanceOf[Timestamp]) {
                // Spark data type is timestamp
                logger.debug(s"Spark type: Timestamp; Kinetica base type: '${columnBaseType}'; Kinetica type: '${columnType}'");
                // Check againt base type
                var sinceepoch = classOf[Timestamp].cast(rtemp).getTime()
                columnBaseType match {
                    case Type.Column.ColumnBaseType.STRING => {
                        if (columnType == Type.Column.ColumnType.DATETIME) {
                            logger.debug("Column type DateTime");
                            try {
                                val result = Instant.ofEpochMilli(sinceepoch).atOffset(zoneID.getRules().getOffset(Instant.now()));
                                genericRecord.putDateTime( columnName,  result.toString(), timeZone );
                            } catch {
                                case e: Exception => {
                                    isColumnValid = false;
                                }
                            }
                        } else if (columnType == Type.Column.ColumnType.DATE) {
                            logger.debug("Column type Date");
                            try {
                                val result = Instant.ofEpochMilli(sinceepoch).atOffset(zoneID.getRules().getOffset(Instant.now()));
                                genericRecord.putDateTime( columnName,  result.toString(), timeZone );
                            } catch {
                                case e: Exception => {
                                    isColumnValid = false;
                                }
                            }
                        } else if (columnType == Type.Column.ColumnType.TIME) {
                            isColumnValid = false;
                            logger.warn(s"Can't convert column '${columnName}' from type java.lang.Long to type '${columnType}'.");

                        } else {
                            logger.debug("Value type Long to common String");
                            genericRecord.put( columnName,  classOf[java.lang.Long].cast(rtemp).toString() );
                        }

                        // // Put in the value as a string
                        // var stringValue: String = rtemp.toString();
                        // genericRecord.putDateTime( columnName, stringValue, timeZone );
                    }
                    case Type.Column.ColumnBaseType.LONG => {
                        // Put in the timestamp value as a long
                        if (columnType == Type.Column.ColumnType.TIMESTAMP) {
                            genericRecord.put(columnName, getValidTimestamp(sinceepoch))
                        } else {
                            genericRecord.put(columnName, sinceepoch)
                        }
                    }
                    case _ => {
                        // generic long value
                        genericRecord.put(columnName, sinceepoch)
                    }
                }
            } else if (rtemp.isInstanceOf[java.sql.Date]) {
                // Spark data type is date
                logger.debug("Date instance")
                genericRecord.put(columnName, rtemp.toString)
            } else if (rtemp.isInstanceOf[java.lang.Boolean]) {
                // Spark data type is bool
                logger.debug("Boolean instance xxx " + toBoolean(rtemp))
                genericRecord.put(columnName, bool2int(toBoolean(rtemp)))
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
                        isColumnValid = false;
                    }
                }
            } else if (rtemp.isInstanceOf[java.lang.Short]) {
                // Spark data type is short
                logger.debug("Short")
                genericRecord.put(columnName, classOf[java.lang.Short].cast(rtemp).intValue())
            } else if (rtemp.isInstanceOf[java.lang.Float]) {
                // Spark data type is float
                logger.debug(s"Spark type: Float; Kinetica base type: '${columnBaseType}'; Kinetica type: '${columnType}'");
                // Check against the base type
                columnBaseType match {
                    case Type.Column.ColumnBaseType.FLOAT => {
                        genericRecord.put(columnName, classOf[java.lang.Float].cast(rtemp).floatValue());
                    }
                    case Type.Column.ColumnBaseType.DOUBLE => {
                        genericRecord.put(columnName, toDouble(rtemp));
                    }
                    case Type.Column.ColumnBaseType.STRING => {
                        genericRecord.put(columnName, rtemp.toString());
                    }
                    case _ => {
                        logger.debug(s"Unhandled Kinetica column type: '${columnType}'") ;
                        isColumnValid = false;
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
            } else if (rtemp.isInstanceOf[java.lang.Byte]) {
                // Spark data type is byte
                logger.debug("Byte")
                genericRecord.put( columnName,
                                   classOf[java.lang.Byte].cast(rtemp).intValue());
                // classOf[Byte].cast(rtemp).intValue())
            } else if ( rtemp.isInstanceOf[java.lang.String]
                        || rtemp.isInstanceOf[org.apache.spark.unsafe.types.UTF8String] ) {
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
                        logger.debug("String to long conversion");
                        if (java.util.regex.Pattern.matches(LONG_REGEX, rtemp.toString())) {
                            genericRecord.put(columnName, rtemp.toString().toLong);
                        } else {
                            // Parsing the String DateTime value into a Long Timestamp
                            logger.debug("Datetime to Timestamp conversion");
                            // Get the date as a number of milliseconds since the epoch
                            try {
                                val sinceepoch = parseDatetimeToTimestamp( rtemp.toString(),
                                                                           TIMESTAMP_FORMATTER );
                                genericRecord.put(columnName, sinceepoch);
                            } catch {
                                case e: Exception => {
                                    isColumnValid = false;
                                }
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
                                logger.debug("String to datetime conversion");

                                if (java.util.regex.Pattern.matches(LONG_REGEX, rtemp.toString())) {
                                    // long timestamp to datetime/date/time
                                    // convert the timestamp to LocalDateTime
                                    val result = convertTimestampToDatetime(rtemp.toString().toLong)
                                    genericRecord.putDateTime( columnName,  result.toString(), timeZone );
                                } else {
                                    // Parsing the String value into String DateTime or Date or Time
                                    logger.debug("Date String to Date format conversion");
                                    try {
                                        // If the string literal represents a zoned datetime or zoned time,
                                        // the value has to be converted to "UTC" timezone before applying
                                        // the timeZone from global setting, and value format is chosen by
                                        // destination columnType
                                        val result: LocalDateTime = convertDateStringToLocalDateTimeUTC( rtemp.toString(),
                                                                                                         TIMESTAMP_FORMATTER )
                                            .toLocalDateTime();
                                        if ( columnType == Type.Column.ColumnType.DATETIME ) {
                                            genericRecord.putDateTime( columnName,  result.toString(), timeZone );
                                        }
                                        if ( columnType == Type.Column.ColumnType.DATE ) {
                                            genericRecord.putDateTime( columnName,  result.toLocalDate().toString(), timeZone );
                                        }
                                        if ( columnType == Type.Column.ColumnType.TIME ) {
                                            genericRecord.putDateTime( columnName,  result.toLocalTime().toString(), timeZone );
                                        }
                                    } catch {
                                        case e: Exception => {
                                            try {
                                                genericRecord.putDateTime( columnName,  rtemp.toString(), timeZone );
                                            } catch {
                                                case e: Exception => {
                                                    logger.debug(s"String to ${columnType} conversion failed for column '${columnName}'");
                                                    isColumnValid = false;
                                                }
                                            }
                                        }
                                    }
                                }
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
            } else if (rtemp.isInstanceOf[Array[Byte]]) {
                // Spark data type is byte array
                logger.debug(s"Byte array found, column type is ${columnType}");
                logger.debug("Byte array length = " + rtemp.asInstanceOf[Array[Byte]].length);
                genericRecord.put(columnName, ByteBuffer.wrap(rtemp.asInstanceOf[Array[Byte]]));
            } else {
                logger.debug("Spark type {} Kinetica instance type is {} ", rtemp.getClass(), column.getType);
                genericRecord.put( columnName,
                                   //column.getType.cast(rtemp))
                                   rtemp.toString() );
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
