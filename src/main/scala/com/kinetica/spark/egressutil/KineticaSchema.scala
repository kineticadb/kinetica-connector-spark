package com.kinetica.spark.egressutil;

import java.sql.{ ResultSetMetaData, Connection, SQLException, Types };
import java.util.Properties;
import org.apache.spark.sql.types._;

import com.gpudb.ColumnProperty;
import com.gpudb.Record;
import com.gpudb.Type;
import com.gpudb.Type._;

import com.kinetica.spark.LoaderParams;
import com.kinetica.spark.util.Constants;
import com.kinetica.spark.util.table.SparkKineticaTableUtil;

/**
 * Generates schema for the data source by mapping to Kinetica jdbc type to Spark sql data types.
 * Spark SQL jdbc datasource mapping is modified to make Kinetica specific.
 */
private[kinetica] object KineticaSchema {

    /**
     *
     * Returns spark sql type schema based the given table.
     *
     * @param url  Connection url to the netezzza database.
     * @param properties  connection properties.
     * @param table The table name of the desired table.
     * @return A StructType giving the table's spark sql schema.
     * @throws SQLException if the table specification is garbage.
     * @throws SQLException if the table contains an unsupported type.
     */
    def getSparkSqlSchema( url: String,
                           properties: LoaderParams,
                           table: String,
                           throwIfNotExists: Boolean): Option[StructType] = {

        val tableExists = SparkKineticaTableUtil.hasTable( table, properties );
        if ( !tableExists ) {
            if ( throwIfNotExists ) {
                throw new RuntimeException( s"Table '$table' does not exist!" );
            } else {
                // Can't create a schema if the table doesn't exist
                return None;
            }
        }

        //println(s"########## Getting schema for table ${table} ############# ")
        try {
            val typeColumns = Type.fromTable(properties.getGpudb(), table).getColumns
        } catch {
            // Could not retrieve any table, probably because the table doesn't exist
            case re: RuntimeException => None
            case e: Exception => None
        }

        val conn: Connection = KineticaJdbcUtils.getConnector(url, properties)()
        try {
            var quotedTableName = KineticaJdbcUtils.quoteTableName(table);
            // Need to quote the table name, but quotess don't work with string
            // interpolation in scala; the following is correct, though ugly
            val selectQuery = s"""SELECT * FROM "$quotedTableName" limit 1""";


            val rs = conn.prepareStatement( selectQuery ).executeQuery()
            try {
                val rsmd = rs.getMetaData
                val ncols = rsmd.getColumnCount
                val fields = new Array[StructField](ncols)
                var i = 0
                while (i < ncols) {
                    val columnName = rsmd.getColumnLabel(i + 1)
                    val dataType = rsmd.getColumnType(i + 1)
                    val typeName = rsmd.getColumnTypeName(i + 1)
                    val fieldSize = rsmd.getPrecision(i + 1)
                    val fieldScale = rsmd.getScale(i + 1)
                    val isSigned = rsmd.isSigned(i + 1)
                    val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
                    val columnType = getSparkSqlType(dataType, fieldSize, fieldScale, isSigned)

                    val metadata = new MetadataBuilder().putString("name", columnName)

                    fields(i) = StructField(columnName, columnType, nullable, metadata.build())
                    i = i + 1
                }
                val schema = new StructType(fields)
                Option.apply( schema )
            } finally {
                rs.close()
            }
        } finally {
            conn.close()
        }
    }  // end getSparkSqlSchema


    /**
     *
     * Returns spark type schema based on the given Record object.
     *
     * @param record  GPUdb Java API Record object
     *
     * @return A StructType giving the record's type.
     * @throws SQLException if the table specification is garbage.
     * @throws SQLException if the table contains an unsupported type.
     */
    def getRecordSchema(record: Record): StructType = {

        // Get the columns of the record's type
        val columns = record.getType().getColumns();
        val ncols = columns.size();

        // Create the fields based on the columns
        val fields = new Array[StructField]( ncols )
        for ( i <- 0 until ncols ) {
            val column     = columns.get( i );
            val columnName = column.getName();
            val nullable   = column.isNullable();

            // The following column information will be deduced
            // based on the column's avro type and Kinetica properties
            var dataType   : Int     = 0;
            var typeName   : String  = null;
            var fieldSize  : Int     = 0;
            var fieldScale : Int     = 0;
            var isSigned   : Boolean = false;

            val columnAvroType = column.getType();
            val columnProperties = column.getProperties();

            // Figure out the rest of the properties based on the field's
            // avro type and properties
            columnAvroType match {

                // Double
                case q if (q == classOf[java.lang.Double]) =>
                {
                    dataType   = java.sql.Types.DOUBLE;
                    // Note: Ideally, would've used 'classOf[ java.sql.Types.DOUBLE ].getName()'
                    // on the RHS, but scala can't handle static fields, so using
                    // raw strings for the type names
                    typeName   = "DOUBLE";
                    fieldSize  = 53;
                    fieldScale = 0; // TODO: Confirm that this is correct
                    isSigned   = true;
                }

                // Float
                case q if (q == classOf[java.lang.Float]) =>
                {
                    dataType   = java.sql.Types.FLOAT;
                    typeName   = "REAL";
                    fieldSize  = 24;
                    fieldScale = 0; // TODO: Confirm that this is correct
                    isSigned   = true;
                 }

                // Integer
                case q if (q == classOf[java.lang.Integer]) =>
                {
                    isSigned   = true;
                    fieldScale = 0; // TODO: Confirm that this is correct

                    // Check subtypes
                    if ( columnProperties.contains( ColumnProperty.INT8 ) ) {
                        // Byte type
                        dataType   = java.sql.Types.TINYINT;
                        typeName   = "TINYINT (int8)";
                        fieldSize  = 3;
                    } else if ( columnProperties.contains( ColumnProperty.INT16 ) ) {
                        // Short type
                        dataType   = java.sql.Types.SMALLINT;
                        typeName   = "SMALLINT (int16)";
                        fieldSize  = 5;
                    } else {
                        // Regular integer
                        dataType   = java.sql.Types.INTEGER;
                        typeName   = "INTEGER";
                        fieldSize  = 10;
                    }
                }

                // Long
                case q if (q == classOf[java.lang.Long]) =>
                {
                    // Check subtypes
                    if ( columnProperties.contains( ColumnProperty.TIMESTAMP ) ) {
                        // Timestamp type
                        dataType   = java.sql.Types.TIMESTAMP;
                        typeName   = "TYPE_TIMESTAMP";
                        fieldSize  = 26;
                        fieldScale = 0; // TODO: Confirm that this is correct
                        isSigned   = true;
                    } else {
                        // Regular longs
                        dataType   = java.sql.Types.BIGINT;
                        typeName   = "BIGINT";
                        fieldSize  = 19;
                        fieldScale = 0; // TODO: Confirm that this is correct
                        isSigned   = true;
                    }
                }

                // String
                case q if (q == classOf[java.lang.String]) =>
                {
                    // Check for subtypes
                    if ( columnProperties.contains( ColumnProperty.DECIMAL ) ) {
                        // Decimal type
                        dataType   = java.sql.Types.DECIMAL;
                        typeName   = "DECIMAL";
                        fieldSize  = Constants.KINETICA_DECIMAL_PRECISION;
                        fieldScale = Constants.KINETICA_DECIMAL_SCALE;
                        isSigned   = true;
                    } else if ( columnProperties.contains( ColumnProperty.DATE ) ) {
                        // Date type (convert to date)
                        dataType   = java.sql.Types.DATE;
                        typeName   = "TYPE_DATE";
                        fieldSize  = 10;
                        fieldScale = 0;
                        isSigned   = false;
                    } else if ( columnProperties.contains( ColumnProperty.TIME ) ) {
                        // Time type (convert to time)
                        dataType   = java.sql.Types.TIME;
                        typeName   = "TYPE_TIME";
                        fieldSize  = 8;
                        fieldScale = 0;
                        isSigned   = false;
                    } else if ( columnProperties.contains( ColumnProperty.DATETIME ) ) {
                        // Datetime type (convert to timestamp)
                        dataType   = java.sql.Types.TIMESTAMP;
                        typeName   = "TYPE_TIMESTAMP";
                        fieldSize  = 26;
                        fieldScale = 0;
                        isSigned   = false;
                    } else if ( columnProperties.contains( ColumnProperty.IPV4 ) ) {
                        // IPv4 type
                        dataType   = java.sql.Types.VARCHAR;
                        typeName   = "VARCHAR (ipv4)";
                        fieldSize  = 15;
                        fieldScale = 0;
                        isSigned   = false;
                    } else if ( columnProperties.contains( ColumnProperty.WKT ) ) {
                        // WKT type
                        dataType   = java.sql.Types.VARCHAR;
                        typeName   = "VARCHAR (wkt)";
                        fieldSize  = Constants.KINETICA_MAX_STRING_SIZE;
                        fieldScale = 0;
                        isSigned   = false;
                    } else if ( columnProperties.contains( ColumnProperty.CHAR1 ) ) {
                        // char1 type
                        dataType   = java.sql.Types.VARCHAR;
                        typeName   = "VARCHAR";
                        fieldSize  = Constants.KINETICA_CHAR1_SIZE;
                        fieldScale = 0;
                        isSigned   = false;
                    } else if ( columnProperties.contains( ColumnProperty.CHAR2 ) ) {
                        // char2 type
                        dataType   = java.sql.Types.VARCHAR;
                        typeName   = "VARCHAR";
                        fieldSize  = Constants.KINETICA_CHAR2_SIZE;
                        fieldScale = 0;
                        isSigned   = false;
                    } else if ( columnProperties.contains( ColumnProperty.CHAR4 ) ) {
                        // char4 type
                        dataType   = java.sql.Types.VARCHAR;
                        typeName   = "VARCHAR";
                        fieldSize  = Constants.KINETICA_CHAR4_SIZE;
                        fieldScale = 0;
                        isSigned   = false;
                    } else if ( columnProperties.contains( ColumnProperty.CHAR8 ) ) {
                        // char8 type
                        dataType   = java.sql.Types.VARCHAR;
                        typeName   = "VARCHAR";
                        fieldSize  = Constants.KINETICA_CHAR8_SIZE;
                        fieldScale = 0;
                        isSigned   = false;
                    } else if ( columnProperties.contains( ColumnProperty.CHAR16 ) ) {
                        // char16 type
                        dataType   = java.sql.Types.VARCHAR;
                        typeName   = "VARCHAR";
                        fieldSize  = Constants.KINETICA_CHAR16_SIZE;
                        fieldScale = 0;
                        isSigned   = false;
                    } else if ( columnProperties.contains( ColumnProperty.CHAR32 ) ) {
                        // char32 type
                        dataType   = java.sql.Types.VARCHAR;
                        typeName   = "VARCHAR";
                        fieldSize  = Constants.KINETICA_CHAR32_SIZE;
                        fieldScale = 0;
                        isSigned   = false;
                    } else if ( columnProperties.contains( ColumnProperty.CHAR64 ) ) {
                        // char64 type
                        dataType   = java.sql.Types.VARCHAR;
                        typeName   = "VARCHAR";
                        fieldSize  = Constants.KINETICA_CHAR64_SIZE;
                        fieldScale = 0;
                        isSigned   = false;
                    } else if ( columnProperties.contains( ColumnProperty.CHAR128 ) ) {
                        // char128 type
                        dataType   = java.sql.Types.VARCHAR;
                        typeName   = "VARCHAR";
                        fieldSize  = Constants.KINETICA_CHAR128_SIZE;
                        fieldScale = 0;
                        isSigned   = false;
                    } else if ( columnProperties.contains( ColumnProperty.CHAR256 ) ) {
                        // char256 type
                        dataType   = java.sql.Types.VARCHAR;
                        typeName   = "VARCHAR";
                        fieldSize  = Constants.KINETICA_CHAR256_SIZE;
                        fieldScale = 0;
                        isSigned   = false;
                    } else {
                        // Regular string type
                        dataType   = java.sql.Types.VARCHAR;
                        typeName   = "VARCHAR";
                        fieldSize  = Constants.KINETICA_MAX_STRING_SIZE;
                        fieldScale = 0;
                        isSigned   = false;
                    }
                }   // end string type

                // Bytes
                case q if (q == classOf[java.nio.ByteBuffer]) =>
                {
                    dataType   = java.sql.Types.BINARY;
                    typeName   = "BINARY (store_only)";
                    fieldSize  = 0;  // TODO: Confirm that this is what we need
                    fieldScale = 0;
                    isSigned   = false;
                }
            }

            val columnType = getSparkSqlType(dataType, fieldSize, fieldScale, isSigned);

            val metadata = new MetadataBuilder().putString("name", columnName);

            fields( i ) = StructField(columnName, columnType, nullable, metadata.build());
        }

        // Create and return the schema based on the fields
        val schema = new StructType( fields );
        schema
    }  // end getRecordSchema


    def reorderSchemaFieldIndex(columns: java.util.List[Column], columnName: String): Int = {
        val it = columns.iterator()
        var ii = 0;
        while( it.hasNext() ) {
            if( it.next.getName.equals(columnName) ) {
                return ii
            }
            ii = ii + 1
        }
        throw new RuntimeException(" Schema field not found -> " + columnName)
    }

    /**
     * Maps JDBC type to a spark sql type.
     *
     * @param jdbcType - JDBC type for the Kinetica column
     * @return The Spark SQL type corresponding to sqlType.
     */
    def getSparkSqlType(
        jdbcType: Int,
        precision: Int,
        scale: Int,
        signed: Boolean): DataType = {
        val answer = jdbcType match {
            // scalastyle:off
            case java.sql.Types.BIGINT => if (signed) {
                LongType
            } else {
                DecimalType(20, 0)
            }
            case java.sql.Types.BINARY => BinaryType
            case java.sql.Types.BIT => BooleanType
            case java.sql.Types.BOOLEAN => BooleanType
            case java.sql.Types.CHAR => StringType
            case java.sql.Types.DATE => DateType
            case java.sql.Types.DECIMAL if precision != 0 || scale != 0 => DecimalType(precision, scale)
            case java.sql.Types.DECIMAL => DecimalType(38, 18) // Spark 1.5.0 default
            case java.sql.Types.DOUBLE => DoubleType
            case java.sql.Types.FLOAT => FloatType
            // case java.sql.Types.FLOAT => DoubleType
            case java.sql.Types.INTEGER => if (signed) {
                IntegerType
            } else {
                LongType
            }
            case java.sql.Types.JAVA_OBJECT => null
            case java.sql.Types.LONGNVARCHAR => StringType
            case java.sql.Types.LONGVARBINARY => BinaryType
            case java.sql.Types.LONGVARCHAR => StringType
            case java.sql.Types.NCHAR => StringType
            case java.sql.Types.NUMERIC if precision != 0 || scale != 0 => DecimalType(precision, scale)
            case java.sql.Types.NUMERIC => DecimalType(38, 18) // Spark 1.5.0 default
            case java.sql.Types.NVARCHAR => StringType
            case java.sql.Types.OTHER => null
            case java.sql.Types.REAL => FloatType
            case java.sql.Types.ROWID => LongType
            case java.sql.Types.SMALLINT => ShortType
            // case java.sql.Types.SMALLINT => IntegerType
            case java.sql.Types.STRUCT => StringType
            // For time, use string instead of timestamp since the latter
            // prepends the CURRENT date to the date
            case java.sql.Types.TIME => StringType
            case java.sql.Types.TIMESTAMP => TimestampType
            case java.sql.Types.TINYINT => ByteType
            // case java.sql.Types.TINYINT => IntegerType
            case java.sql.Types.VARBINARY => BinaryType
            case java.sql.Types.VARCHAR => StringType
            case _ => null
            // scalastyle:on
        }

        if (answer == null) throw new SQLException("Unsupported type " + jdbcType)
        answer
    }

    /**
     * Prune all but the specified columns from the Spark SQL schema.
     *
     * @param schema - The Spark sql schema of the mapped kinetica table
     * @param columns - The list of desired columns
     *
     * @return A Spark sql schema corresponding to columns in the given order.
     */
    def pruneSchema(schema: Option[StructType], columns: Array[String]): StructType = {

        // Check that the incoming schema exists
        schema match {
            case Some(_) => Unit; // Nothing to do if the schema exists
            case None => throw new RuntimeException( "Must be given a valid schema; given None; " );
        }

        val fieldMap = Map(schema.get.fields map { x => x.metadata.getString("name") -> x }: _*)
        val newSchema = new StructType(columns map { name => fieldMap(name) })
        newSchema
    }
}
