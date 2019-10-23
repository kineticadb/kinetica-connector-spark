package com.kinetica.spark.egressutil;

// MUCH OF THIS CODE IS LIFTED FROM SPARK CODEBASE - SRB

import java.sql.ResultSet;
import java.text.SimpleDateFormat;

import com.typesafe.scalalogging.LazyLogging;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters._;
import scala.util.Random;

import com.gpudb.ColumnProperty;
import com.gpudb.GPUdb;
import com.gpudb.Record;
import com.gpudb.Type;
import com.gpudb.protocol.GetRecordsByColumnRequest;
import com.gpudb.protocol.GetRecordsByColumnResponse;
import com.gpudb.protocol.ShowTableRequest;
import com.gpudb.protocol.ShowTableResponse;

import com.kinetica.spark.util.Constants;




/**
 * Utility functions for retrieving records via the JDBC connector.  To be
 * used when filters need to be pushed down to the database.
 */
object KineticaEgressUtilsJdbc extends LazyLogging {

    private type JDBCValueGetter = (ResultSet, InternalRow, Int) => Unit

    private[spark] def resultSetToSparkInternalRows(
        resultSet: ResultSet,
        schema: StructType): Iterator[InternalRow] = {

        new NextIterator[InternalRow] {
            private[this] val rs = resultSet
            private[this] val getters: Array[JDBCValueGetter] = makeGetters(schema)
            private[this] val mutableRow = new SpecificInternalRow(schema.fields.map(x => x.dataType))

            override protected def close(): Unit = {
                try {
                    rs.close()
                } catch {
                    case e: Exception => logger.warn("Exception closing resultset", e)
                }
            }
            
            override protected def getNext(): InternalRow = {
                if (rs.next()) {
                      var i = 0
                      while (i < getters.length) {
                           getters(i).apply(rs, mutableRow, i)
                           if (rs.wasNull) mutableRow.setNullAt(i)
                           i = i + 1
                      }
                      mutableRow
                } else {
                      finished = true
                      null.asInstanceOf[InternalRow]
                }
            }
        }
    }

    private def makeGetters(schema: StructType): Array[JDBCValueGetter] =
        schema.fields.map(sf => makeGetter(sf.dataType, sf.metadata))

    private def makeGetter(dt: DataType, metadata: Metadata): JDBCValueGetter = dt match {
        case BooleanType =>
            (rs: ResultSet, row: InternalRow, pos: Int) =>
                row.setBoolean(pos, rs.getBoolean(pos + 1))

        case DateType =>
            (rs: ResultSet, row: InternalRow, pos: Int) =>
                // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
                val dateVal = rs.getDate(pos + 1)
                if (dateVal != null) {
                    row.setInt(pos, DateTimeUtils.fromJavaDate(dateVal))
                } else {
                    row.update(pos, null)
                }

        // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
        // object returned by ResultSet.getBigDecimal is not correctly matched to the table
        // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
        // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
        // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
        // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
        // retrieve it, you will get wrong result 199.99.
        // So it is needed to set precision and scale for Decimal based on JDBC metadata.

        case decimalType: DecimalType =>
        	(rs: ResultSet, row: InternalRow, pos: Int) =>
    	{
    		val bigD = rs.getBigDecimal(pos + 1)
    		val decimal =  nullSafeConvert2[java.math.BigDecimal, Decimal](bigD, d => Decimal(d, decimalType.precision, decimalType.scale))
    		if(decimal.isDefined)
    		{
    			row.setDecimal(pos, decimal.get, decimalType.precision)
    		} else if(bigD != null) {
    			throw new Exception(s"unable to convert $bigD to Spark Decimal")
    		}
    	}
                
        case DoubleType =>
            (rs: ResultSet, row: InternalRow, pos: Int) =>
                row.setDouble(pos, rs.getDouble(pos + 1))

        case FloatType =>
            (rs: ResultSet, row: InternalRow, pos: Int) =>
                row.setFloat(pos, rs.getFloat(pos + 1))

        case IntegerType =>
            (rs: ResultSet, row: InternalRow, pos: Int) =>
                row.setInt(pos, rs.getInt(pos + 1))

        case LongType if metadata.contains("binarylong") =>
            (rs: ResultSet, row: InternalRow, pos: Int) =>
                val bytes = rs.getBytes(pos + 1)
                var ans = 0L
                var j = 0
                while (j < bytes.length) {
                    ans = 256 * ans + (255 & bytes(j))
                    j = j + 1
                }
                row.setLong(pos, ans)

        case LongType =>
            (rs: ResultSet, row: InternalRow, pos: Int) =>
                row.setLong(pos, rs.getLong(pos + 1))

        case ShortType =>
            (rs: ResultSet, row: InternalRow, pos: Int) =>
                row.setShort(pos, rs.getShort(pos + 1))

        case StringType =>
            (rs: ResultSet, row: InternalRow, pos: Int) =>
                // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
                row.update(pos, UTF8String.fromString(rs.getString(pos + 1)))

        case TimestampType =>
            (rs: ResultSet, row: InternalRow, pos: Int) =>
                val t = rs.getTimestamp(pos + 1)
                if (t != null) {
                    row.setLong(pos, DateTimeUtils.fromJavaTimestamp(t))
                } else {
                    row.update(pos, null)
                }

        case BinaryType =>
            (rs: ResultSet, row: InternalRow, pos: Int) =>
                row.update(pos, rs.getBytes(pos + 1))

        case ArrayType(et, _) =>
            val elementConversion = et match {
                case TimestampType =>
                    (array: Object) =>
                        array.asInstanceOf[Array[java.sql.Timestamp]].map { timestamp =>
                            nullSafeConvert(timestamp, DateTimeUtils.fromJavaTimestamp)
                        }

                case StringType =>
                    (array: Object) =>
                        // some underling types are not String such as uuid, inet, cidr, etc.
                        array.asInstanceOf[Array[java.lang.Object]]
                            .map(obj => if (obj == null) null else UTF8String.fromString(obj.toString))

                case DateType =>
                    (array: Object) =>
                        array.asInstanceOf[Array[java.sql.Date]].map { date =>
                            nullSafeConvert(date, DateTimeUtils.fromJavaDate)
                        }

                case dt: DecimalType =>
                    (array: Object) =>
                        array.asInstanceOf[Array[java.math.BigDecimal]].map { decimal =>
                            nullSafeConvert[java.math.BigDecimal](
                                decimal, d => Decimal(d, dt.precision, dt.scale))
                        }

                case LongType if metadata.contains("binarylong") =>
                    throw new IllegalArgumentException(s"Unsupported array element " +
                        s"type ${dt.simpleString} based on binary")

                case ArrayType(_, _) =>
                    throw new IllegalArgumentException("Nested arrays unsupported")

                case _ => (array: Object) => array.asInstanceOf[Array[Any]]
            }

            (rs: ResultSet, row: InternalRow, pos: Int) =>
                val array = nullSafeConvert[java.sql.Array](
                    input = rs.getArray(pos + 1),
                    array => new GenericArrayData(elementConversion.apply(array.getArray)))
                row.update(pos, array)

        case _ => throw new IllegalArgumentException(s"Unsupported type ${dt.simpleString}")
    }

    private def nullSafeConvert2[T, R](input: T, f: T => R): Option[R] = {
    	if (input == null) {
    		None
    	} else {
    		Some(f(input))
    	}
    }

    
    private def nullSafeConvert[T](input: T, f: T => Any): Any = {
        if (input == null) {
            null
        } else {
            f(input)
        }
    }
}   // end KineticaEgressUtilsJdbc



/**
 * Utility functions for retrieving records via the JDBC connector.  To be
 * used when NO filters need to be pushed down to the database.  This path
 * is a lot faster than going through the JDBC connector.
 */
object KineticaEgressUtilsNativeClient extends LazyLogging {

    private type JDBCValueGetter = (ResultSet, Record, InternalRow, Int) => Unit;


    private[spark] def resultSetToSparkInternalRows(
        resultSet: ResultSet,
        resp:      java.util.List[Record] ): Iterator[InternalRow] = {


        // If no Record is passed in, create and return an empty iterator
        if (resp.isEmpty) {
            // This has to be lazy, or `finished` will be set to true, which
            // makes the RDD think there are no records!
            lazy val emptyIterator : NextIterator[InternalRow] = new  NextIterator[InternalRow] {
                override protected def close(): Unit = {
                }
                
                override protected def getNext(): InternalRow = {
                    finished = true
                    null.asInstanceOf[InternalRow]
                }
            }
            return emptyIterator
        }

        // Process a non-empty list of Records to get type etc. information
        val recordType = resp.get( 0 ).getType();
        val recordSchema = KineticaSchema.getRecordSchema( resp.get( 0 ) );

        // Create and return an iterator over the passed in Record objects
        new NextIterator[InternalRow] {
            private[this] val rs = resultSet
            private[this] val dataArrIt = resp.iterator
            private[this] val getters: Array[JDBCValueGetter] = makeGetters( recordType )
            
            private[this] val mutableRow = {
                new SpecificInternalRow( recordSchema.fields.map(x => {
                            x.dataType}) );
            }

            override protected def close(): Unit = {
            }

            // Main fetch loop which is called by Spark to retrieve an internal row
            override protected def getNext(): InternalRow = {
                if ( dataArrIt.hasNext ) {
                    var i = 0
                    val gr = dataArrIt.next
                    while (i < getters.length) {
                        getters(i).apply(rs, gr, mutableRow, i)
                        i = i + 1
                    }
                    mutableRow
                } else {
                    finished = true
                    null.asInstanceOf[InternalRow]
                }
            }
        }
    }

    private def makeGetters(record_type: com.gpudb.Type): Array[JDBCValueGetter] = {
        // Create getter functions per column
        val jdbc_value_getters = record_type.getColumns().asScala.toArray.map( column => makeGetter(column) )
        jdbc_value_getters
    }

    private def makeGetter( column: com.gpudb.Type.Column): JDBCValueGetter = {
        // Get column information
        val columnType = column.getType();
        val columnProperties = column.getProperties();

        // Create a getter function based on the column's type
        columnType match {

            // Double
            case q if (q == classOf[java.lang.Double]) =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
            {
                // Need to convert to Scala double
                val value = gr.getDouble( pos );
                if( value != null ) {
                    row.setDouble( pos, value.toDouble );
                } else {
                    row.update( pos, null );
                }
            }

            // Float
            case q if (q == classOf[java.lang.Float]) =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
            {
                // Need to convert to Scala float
                val value = gr.getFloat( pos );
                if( value != null ) {
                    row.setFloat( pos, value.toFloat );
                } else {
                    row.update( pos, null );
                }
            }

            // Integer
            case q if (q == classOf[java.lang.Integer]) =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
            {
                // Need to convert to Scala integer
                val value = gr.getInt( pos);
                if( value != null ) {
                    // Check subtypes
                    if ( columnProperties.contains( ColumnProperty.INT8 ) ) {
                        // Byte type
                        row.setByte(pos, (value.byteValue()));
                    } else if ( columnProperties.contains( ColumnProperty.INT16 ) ) {
                        // Short type
                        row.setShort(pos, (value.shortValue()));
                    } else {
                        // Regular integer
                        row.setInt( pos, value.toInt );
                    }
                } else {
                    row.update( pos, null );
                }
            }

            // Long
            case q if (q == classOf[java.lang.Long]) =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
            {
                // Need to convert to Scala long
                val value = gr.getLong( pos );
                if( value != null ) {
                    // Check subtypes
                    if ( columnProperties.contains( ColumnProperty.TIMESTAMP ) ) {
                        // Timestamp type
                        val t : java.sql.Timestamp = new java.sql.Timestamp( value );
                        row.setLong( pos, DateTimeUtils.fromJavaTimestamp(t) );
                    } else {
                        // Regular longs
                        row.setLong( pos, value.toLong );
                    }
                } else {
                    row.update( pos, null );
                }
            }

            // String
            case q if (q == classOf[java.lang.String]) =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
            {
                val value = gr.getString( pos );
                if( value != null )
                {
                    // Check for subtypes
                    if ( columnProperties.contains( ColumnProperty.DECIMAL ) )
                    {
                        // Decimal type
                        val bigD = new java.math.BigDecimal( value );
                        val decimal =  nullSafeConvert2[java.math.BigDecimal, Decimal](bigD, d => Decimal( d,
                                                                                                           Constants.KINETICA_DECIMAL_PRECISION,
                                                                                                           Constants.KINETICA_DECIMAL_SCALE ));
                        if( decimal.isDefined )
                        {
                            row.setDecimal( pos, decimal.get, Constants.KINETICA_DECIMAL_PRECISION );
                        } else if(bigD != null)
                        {
                            throw new Exception(s"unable to convert $bigD to Spark Decimal");
                        }
                    } else if ( columnProperties.contains( ColumnProperty.DATE ) )
                    {
                        // Date type (convert to date)
                        val dateVal = new SimpleDateFormat("yyyy-MM-dd").parse( value ).getTime;
                        val sqlDate = new java.sql.Date(dateVal);
                        row.setInt(pos, org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaDate(sqlDate));
                    } else if ( columnProperties.contains( ColumnProperty.DATETIME ) )
                    {
                        // Datetime type (convert to timestamp)
                        val sqlTS = DateTimeUtils.stringToTimestamp( UTF8String.fromString( value ) );
                        row.setLong( pos, sqlTS.get );
                    } else
                    {
                        // All other string types, including charN, time and WKT
                        row.update( pos, UTF8String.fromString(value) );
                    }
                } else
                {
                    row.update( pos, null );
                }
            }

            // Bytes
            case q if (q == classOf[java.nio.ByteBuffer]) =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
            {
                val value = gr.getBytes( pos );
                if( value != null ) {
                    row.update( pos, value.array() );
                } else {
                    row.update( pos, null );
                }
            }
        }
    }


    private def makeGetters(schema: StructType): Array[JDBCValueGetter] = {
        val jdbcvg = schema.fields.map(sf => makeGetter(sf.dataType, sf.metadata))
        jdbcvg        
    }

    private def makeGetter(dt: DataType, metadata: Metadata): JDBCValueGetter = {
        dt match {
   
        case BooleanType =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
                if( gr.getInt(pos) != null ) {
                    val bool = if (gr.getInt(pos) == 0) false else true
                    row.setBoolean(pos, bool)
                } else {
                    row.update(pos, null)
                }

        case DateType =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
                // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
                if (gr.getString(pos) != null) {
                    val dateVal = new SimpleDateFormat("yyyy-MM-dd").parse(gr.getString(pos)).getTime
                    val sqlDate = new java.sql.Date(dateVal)
                    row.setInt(pos, org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaDate(sqlDate))
                } else {
                    row.update(pos, null)
                }

        // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
        // object returned by ResultSet.getBigDecimal is not correctly matched to the table
        // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
        // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
        // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
        // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
        // retrieve it, you will get wrong result 199.99.
        // So it is needed to set precision and scale for Decimal based on JDBC metadata.

        case decimalType: DecimalType =>
        	(rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
    	{
    	    if( gr.getString(pos) != null ) {
        	    val bigD = new java.math.BigDecimal(gr.getString(pos))
        		val decimal =  nullSafeConvert2[java.math.BigDecimal, Decimal](bigD, d => Decimal(d, decimalType.precision, decimalType.scale))
        		if(decimal.isDefined)
        		{
        			row.setDecimal(pos, decimal.get, decimalType.precision)
        		} else if(bigD != null) {
        			throw new Exception(s"unable to convert $bigD to Spark Decimal")
        		}
    	    } else {
    	        row.update(pos, null)
    	    }
    	}
                
        case DoubleType =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
                val d = gr.getDouble(pos)
                if( d != null ) {
                    row.setDouble(pos, d)
                } else {
                    row.update(pos, null)
                }

        case FloatType =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
                val f = gr.getFloat(pos)
                if( f != null ) {
                    row.setFloat(pos, f)
                } else {
                    row.update(pos, null)
                }

        case IntegerType =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
                val i = gr.getInt(pos)
                if( i != null ) {
                    row.setInt(pos, i)
                } else {
                    row.update(pos, null)
                }
        
        /*        
        case LongType if metadata.contains("binarylong") =>
            (rs: ResultSet, gr: GenericRecord, row: InternalRow, pos: Int) =>
                val bytes = rs.getBytes(pos + 1)
                var ans = 0L
                var j = 0
                while (j < bytes.length) {
                    ans = 256 * ans + (255 & bytes(j))
                    j = j + 1
                }
                row.setLong(pos, ans)
		*/
                
        case LongType =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
                val l = gr.getLong(pos)
                if( l != null ) {
                    row.setLong(pos, l)
                } else {
                    row.update(pos, null)
                }

        case ShortType =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
                val i = gr.getInt(pos)
                if( i != null ) {
                    row.setShort(pos, (i.shortValue()))
                } else {
                    row.update(pos, null)
                }

        case StringType =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
                // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
                val s = gr.getString(pos)
                if( s != null ) {
                    row.update(pos, UTF8String.fromString(s))
                } else {
                    row.update(pos, null) 
                }

        case TimestampType =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
                val t : java.sql.Timestamp =  if (gr.getLong(pos) != null) new java.sql.Timestamp(gr.getLong(pos)) else null
                if (t != null) {
                    row.setLong(pos, DateTimeUtils.fromJavaTimestamp(t))
                } else {
                    row.update(pos, null)
                }

        case BinaryType =>
            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
                row.update( pos, gr.getBytes(pos).array() );

        case ArrayType(et, _) =>
            val elementConversion = et match {
                case TimestampType =>
                    (array: Object) =>
                        array.asInstanceOf[Array[java.sql.Timestamp]].map { timestamp =>
                            nullSafeConvert(timestamp, DateTimeUtils.fromJavaTimestamp)
                        }

                case StringType =>
                    (array: Object) =>
                        // some underling types are not String such as uuid, inet, cidr, etc.
                        array.asInstanceOf[Array[java.lang.Object]]
                            .map(obj => if (obj == null) null else UTF8String.fromString(obj.toString))

                case DateType =>
                    (array: Object) =>
                        array.asInstanceOf[Array[java.sql.Date]].map { date =>
                            nullSafeConvert(date, DateTimeUtils.fromJavaDate)
                        }

                case dt: DecimalType =>
                    (array: Object) =>
                        array.asInstanceOf[Array[java.math.BigDecimal]].map { decimal =>
                            nullSafeConvert[java.math.BigDecimal](
                                decimal, d => Decimal(d, dt.precision, dt.scale))
                        }

                case LongType if metadata.contains("binarylong") =>
                    throw new IllegalArgumentException(s"Unsupported array element " +
                        s"type ${dt.simpleString} based on binary")

                case ArrayType(_, _) =>
                    throw new IllegalArgumentException("Nested arrays unsupported")

                case _ => (array: Object) => array.asInstanceOf[Array[Any]]
            }

            (rs: ResultSet, gr: Record, row: InternalRow, pos: Int) =>
                val array = nullSafeConvert[java.sql.Array](
                    input = rs.getArray(pos + 1),
                    array => new GenericArrayData(elementConversion.apply(array.getArray)))
                row.update(pos, array)

        case _ => throw new IllegalArgumentException(s"Unsupported type ${dt.simpleString}")
    }
        
    }

    private def nullSafeConvert2[T, R](input: T, f: T => R): Option[R] = {
    	if (input == null) {
            None
    	} else {
            Some(f(input))
    	}
    }

    
    private def nullSafeConvert[T](input: T, f: T => Any): Any = {
        if (input == null) {
            null
        } else {
            f(input)
        }
    }



    /*
     * Given a GPUdb connection and table name, get the number of rows in the table.
     */
    def getKineticaTableSize(
        gpudbConn:  com.gpudb.GPUdb,
        tableName:  String ): Long = {

        val options: java.util.Map[String, String] = new java.util.HashMap[String, String]();
        options.put( com.gpudb.protocol.ShowTableRequest.Options.GET_SIZES,
                     com.gpudb.protocol.ShowTableRequest.Options.TRUE );

        // Get the table information
        val showTableResponse :  ShowTableResponse = gpudbConn.showTable( tableName, options );

        // Return the table size
        showTableResponse.getTotalFullSize()
    }


    /*
     * Given a GPUdb connection, table name, offset, total number of rows to fetch,
     * and an internal batch size, fetch records from Kinetica using its native
     * Java client API and return the records.  Any given filter will be applied java
     * side.
     */
    def getRecordsFromKinetica(
        gpudbConn:  com.gpudb.GPUdb,
        tableName:  String,
        columns:    Array[String],
        startRow:   Long,
        numRows:    Long,
        batchSize:  Long ): java.util.ArrayList[Record] = {

        val allRecords = new java.util.ArrayList[Record]();

        // Check if the given offset even requires fetching any record, or if it's
        // too large (in which case, we return an empty list)
        // Get the table size
        val tableSize = KineticaEgressUtilsNativeClient.getKineticaTableSize( gpudbConn, tableName );

        if ( startRow >= tableSize ) {
            return allRecords;
        }

        val blank_options: java.util.Map[String, String] = new java.util.HashMap[String, String]();

        // Select the columns to be fetched
        val column_names = new java.util.ArrayList[String]();

        // If no column is specified, then we need to get all the columns
        if ( columns.isEmpty ) {
            logger.debug("getRecordsFromKinetica(): No required columns given; fetching all columns in table");
            // Get all the column names for the table from Kinetica
            val tableType = Type.fromTable( gpudbConn, tableName );
            tableType.getColumns().asScala.foreach( col => column_names.add( col.getName() ) );
        } else {
            logger.debug("getRecordsFromKinetica(): required columns given: {}", columns );
            // Use the given column names
            columns.foreach( f => column_names.add( f ) );
        }
        
        // Call /get/records/bycolumn repeatedly with a max rows of batchSize till we have done numRows
        // Collect the list of records in a separate list and pass into resultSetToSparkInternalRows
        // TODO
        
        val extraRows = numRows % batchSize;
        var loopCount = numRows / batchSize;

        var maxRowsToFetch = batchSize
        // Handle the case when the total # of rows to fetch is less than
        // the batch size
        if( loopCount == 0 ) {
            loopCount = 1;
            maxRowsToFetch = 0;
        }

        // Get an ID for this invocation of this function (useful for logging)
        val id = Random.nextInt();
       
        var ii = 0;
        while ( ii < loopCount ) {

            logger.debug("getRecordsFromKinetica(): Function ID {}; while loop iteration #{}", id, ii);
            // Set up the request
            val limit  : Long = if( ii == loopCount -1 ) (maxRowsToFetch + extraRows) else maxRowsToFetch;
            val offset : Long = (startRow + (ii * maxRowsToFetch));
            val getRecordsByColumnReq = new GetRecordsByColumnRequest( tableName, column_names,
                                                                       offset, limit,
                                                                       blank_options );

            // Make the request
            val getRecordsByColumnResponse :  GetRecordsByColumnResponse = gpudbConn.getRecordsByColumn(getRecordsByColumnReq);

            // Get the records out of the response
            allRecords.addAll( getRecordsByColumnResponse.getData );
            ii = ii + 1;
        }
        
        // Return the records
        allRecords
    }   // end getRecordsFromKinetica


    /*
     * Given a GPUdb connection, table name, offset, total number of rows to fetch,
     * and an internal batch size, fetch records from Kinetica using its native
     * Java client API.  Encode the records into Row so that spark can understand them
     * using the given schema and return the rows.
     */
    def getRowsFromKinetica(
        gpudbConn:  com.gpudb.GPUdb,
        tableName:  String,
        columns:    Array[String],
        schema:     StructType,
        startRow:   Long,
        numRows:    Long,
        batchSize:  Long ): Iterator[Row] = {

        logger.debug("KineticaEgressUtilsNativeClient::getRowsFromKinetica() startRow {} numRows {}", startRow, numRows);
        
        // Fetch the records from Kinetica in the native format
        val allRecords = getRecordsFromKinetica( gpudbConn, tableName, columns,
                                                 startRow, numRows, batchSize )

        // Note: Beware of calling .size or other functions on the rows created below
        //       (even in a debug print); it may have unintended consequences based
        //       on where this function is being called from.  For example, KineticaRDD's
        //       compute() calls this, and due to a debug print with ${myRows.size} in it,
        //       the RDD was ALWAYS thought to be empty.  Bottom line: do NOT iterate over
        //       the rows being returned.
        
        // Convert the records so that spark can understand it
        val internalRows = resultSetToSparkInternalRows( null, allRecords )
        val encoder = RowEncoder.apply( schema ).resolveAndBind()
        val myRows = internalRows.map( encoder.fromRow )
        myRows
    }
        

}   // end KineticaEgressUtilsNativeClient
