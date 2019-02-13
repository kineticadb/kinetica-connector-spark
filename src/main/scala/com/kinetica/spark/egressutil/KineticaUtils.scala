package com.kinetica.spark.egressutil

// MUCH OF THIS CODE IS LIFTED FROM SPARK CODEBASE - SRB

import java.sql.ResultSet

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.unsafe.types.UTF8String

import com.gpudb.GPUdb;
import com.gpudb.Record;
import com.gpudb.protocol.GetRecordsByColumnRequest;
import com.gpudb.protocol.GetRecordsByColumnResponse;
import com.gpudb.protocol.ShowTableRequest;
import com.gpudb.protocol.ShowTableResponse;

import java.text.SimpleDateFormat;

object KineticaUtils extends Logging {

    private type JDBCValueGetter = (ResultSet, Record, InternalRow, Int) => Unit




    private[spark] def resultSetToSparkInternalRows(
        resultSet: ResultSet,
        resp: java.util.List[Record],
        schema: StructType): Iterator[InternalRow] = {
        
        new NextIterator[InternalRow] {
            private[this] val rs = resultSet
            private[this] val dataArrIt = resp.iterator
            private[this] val getters: Array[JDBCValueGetter] = makeGetters(schema)
            private[this] val mutableRow = {
                new SpecificInternalRow(schema.fields.map(x => {
                x.dataType}))
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
                row.update(pos, gr.getBytes(pos))

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
     * Given a GPUdb connection, table name, offset, total number of rows to fetch
     * and an internal batch size, fetch records from Kinetica using its native
     * Java client API and return the records.
     */
    def getRecordsFromKinetica(
        gpudbConn:  com.gpudb.GPUdb,
        tableName:  String,
        columns:    Array[String],
        startRow:   Int,
        numRows:    Int,
        batchSize:  Int ): java.util.ArrayList[Record] = {

        val blank_options: java.util.Map[String, String] = new java.util.HashMap[String, String]();

        // Select the columns to be fetched
        val column_names = new java.util.ArrayList[String]();
        columns.foreach(f => column_names.add(f));
        
        
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
        
       
        val allRecords = new java.util.ArrayList[Record]();
        var ii = 0;                       
        while ( ii < loopCount ) {

            // Set up the request
            val limit = if( ii == loopCount -1 ) (maxRowsToFetch + extraRows) else maxRowsToFetch;
            val offset = (startRow + (ii * maxRowsToFetch));
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
    }


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
        startRow:   Int,
        numRows:    Int,
        batchSize:  Int ): Iterator[Row] = {

        // Fetch the records from Kinetica in the native format
        val allRecords = getRecordsFromKinetica( gpudbConn, tableName, columns,
                                                 startRow, numRows, batchSize )

        // Convert the records so that spark can understand it
        val internalRows = resultSetToSparkInternalRows(null, allRecords, schema)
        val encoder = RowEncoder.apply(schema).resolveAndBind()
        val myRows = internalRows.map(encoder.fromRow)
        myRows
    }
        

}
