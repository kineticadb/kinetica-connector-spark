package com.kinetica.spark.loader

import java.util.ArrayList
import java.util.Date
import java.util.HashMap
import java.util.List

import scala.beans.BeanProperty
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.setAsJavaSet
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.StructType

import com.gpudb.ColumnProperty
import com.gpudb.GPUdb
import com.gpudb.GPUdbBase
import com.gpudb.Type
import com.gpudb.Type.Column
import com.gpudb.protocol.CreateTableRequest
import com.gpudb.protocol.ShowTableRequest
import com.gpudb.protocol.ShowTableResponse
import org.apache.spark.Logging

class SchemaManager (conf: LoaderConfiguration) extends Logging {

    private val gpudb: GPUdb = conf.getGpudb
    private val tableName: String = conf.tablename
    private val schemaName: String = conf.schemaname
    private val useTemplates: Boolean = conf.useTemplates

    private var isReplicated: Boolean = false

    @BeanProperty
    var destType: Type = _

    private var destTypeId: String = _

    def setupSchema(loaderConfig: LoaderConfiguration, sparkSchema: StructType): java.util.HashMap[Integer, Integer] = {

        if(this.useTemplates) {
            // lookup schema from template
            this.resolveTemplate()
            if (loaderConfig.truncateTable) {
                this.gpudb.clearTable(this.tableName, null, null)
            }
            this.createTable()
        }
        if (loaderConfig.hasTable()) {
            // use existing schema from table
            val response: ShowTableResponse = this.gpudb.showTable(this.tableName, null)
            this.setTypeFromResponse(response, 0)

            if (loaderConfig.truncateTable) {
                this.gpudb.clearTable(this.tableName, null, null)
                this.createTable()
            }
        }
        else if (loaderConfig.createTable) {
            // convert schema from Spark
            this.destType = convertType(sparkSchema)
            this.destTypeId = this.destType.create(this.gpudb)
            this.createTable()
        }
        else {
            throw new Exception(
                String.format(
                    "Table <%s> does not exist and <table.create = false>.",
                    loaderConfig.tablename))
        }
        loaderConfig.setType(this.getDestType)
        this.getColumnMap(sparkSchema)
    }

    private def createTable(): Unit = {
        logInfo( "Creating table schema/tablename/type=" + this.schemaName + "/" + this.tableName + "/" + this.destTypeId)

        var options : java.util.Map[String, String] = null
        if( conf.tableReplicated ) {
            options = GPUdbBase.options(CreateTableRequest.Options.COLLECTION_NAME, this.schemaName,
                    CreateTableRequest.Options.IS_REPLICATED, CreateTableRequest.Options.TRUE)
        } else {
            options = GPUdbBase.options(CreateTableRequest.Options.COLLECTION_NAME, this.schemaName)
        }

        this.gpudb.createTable(
            this.tableName,
            this.destTypeId,
            options)
    }

    private def setTypeFromResponse(response: ShowTableResponse, index: Int): Unit = {
        this.destTypeId = response.getTypeIds().get(index)

        val typeSchema: String = response.getTypeSchemas.get(index)
        val typeLabel: String = response.getTypeLabels.get(index)
        val typeProps: java.util.Map[String, java.util.List[String]] = response.getProperties.get(index)
        this.destType = new Type(typeLabel, typeSchema, typeProps)

        // check if this table is replicated
        val tableDesc: List[String] = response.getTableDescriptions().get(index)
        if(tableDesc.contains("REPLICATED")) {
            this.isReplicated = tableDesc.contains("REPLICATED")
        }
    }

    private def resolveTemplate(): Unit = {
        val templateSchema: String = this.schemaName + ".template"

        val options: HashMap[String, String] = new HashMap[String, String]()
        options.put(
            ShowTableRequest.Options.SHOW_CHILDREN,
            ShowTableRequest.Options.TRUE)

        val response: ShowTableResponse = this.gpudb.showTable(templateSchema, options)
        val tableNames: List[String] = response.getTableNames
        val prefix: String = this.tableName + "."

        val templateName: String  = tableNames
            .filter(x => x.startsWith(prefix))
            .sorted.lastOption.getOrElse {
                throw new Exception(
                    String.format("Could not find a suitable template in <%s> for table <%s.%s>",
                        templateSchema, this.schemaName, this.tableName))
            }

        val tableIndex: Int = tableNames.indexOf(templateName)
        setTypeFromResponse(response, tableIndex)
        logInfo("Found template table/ID " + templateName + "/" + this.destTypeId)
    }
    
    def adjustSourceSchema(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
        if( conf.csvHeader == false && conf.hasTable) {
            val response: ShowTableResponse = this.gpudb.showTable(this.tableName, null)
            setTypeFromResponse(response, 0)
            val destCols = this.destType.getColumns
            val colNames = destCols.map(t => (t.getName.toUpperCase)).toArray
            val dfRenamed = df.toDF(colNames: _*)
            return dfRenamed
        } 
        return df
    }

    private def getColumnMap(sparkSchema: StructType): HashMap[Integer, Integer] = {
        val sourceType: Type = convertType(sparkSchema)
        val sourceCols = sourceType.getColumns
        val destCols = this.destType.getColumns

        val toMapped = destCols.map(t => (t.getName.toUpperCase, t)).toMap
        var unMappedDest : scala.collection.mutable.Map[String, Column]
            = collection.mutable.Map(toMapped.toSeq: _*)

        val toUnmapped = sourceCols.map(t => (t.getName.toUpperCase, t)).toMap
        val unMappedSource : scala.collection.mutable.Map[String, Column]
            = collection.mutable.Map(toUnmapped.toSeq: _*)

        val columnMap: HashMap[Integer, Integer] = new HashMap[Integer, Integer]()

        for (sourceIdx <- 0 until sourceCols.size) {
            breakable {
                val sourceCol: Column = sourceCols.get(sourceIdx)
                val destCol: Column = toMapped.get(sourceCol.getName.toUpperCase())
                    match {
                    case None => break // don't map
                    case Some(col) => col
                }

                val sourceDT: Class[_] = sourceCol.getType
                val destDT: Class[_] = destCol.getType

                if (!validConversion(sourceDT, destDT)) {
                    throw new Exception(
                        String.format("Could not convert datatype for column <%s>: %s -> %s",
                            sourceCol.getName, sourceDT.getName, destDT.getName))
                }

                val destIdx: Int = this.destType.getColumnIndex(destCol.getName)
                if (destIdx < 0) {
                    // should never happen
                    throw new Exception("Column not found in type");
                }

                // add the mapping
                logInfo("Mapping column colName/srcIdx/destIdx" + sourceCol.getName + "/" + sourceIdx + "/" + destIdx)
                columnMap.put(sourceIdx, destIdx)

                // keep track of columns not mapped
                unMappedDest.remove(destCol.getName.toUpperCase())
                unMappedSource.remove(sourceCol.getName.toUpperCase())
            }
        }

        if (unMappedSource.size > 0) {
            val unMappedCols: String = unMappedSource.keySet.mkString(",")
            logInfo("The following columns in the dataframe were not mapped to table schemaName/tableName/unmappedCols" +
                this.schemaName + "/" + this.tableName + "/" + unMappedCols)
        }
        if (unMappedDest.size > 0) {
            val nullColumnList = unMappedDest.values.filter(x => !x.isNullable()).map(_.getName)

            if (nullColumnList.size > 0) {
                val nullColumns: String = nullColumnList.mkString(",")
                throw new Exception(
                    String.format("The following columns in <%s.%s> are nullable and not mapped: %s",
                        this.schemaName, this.tableName, nullColumns))
            }

            val unMappedCols: String = String.join(", ", unMappedDest.keySet)
            logInfo("The following columns in table schemaName/tableName/unmappedCols" +
                this.schemaName + "/" + this.tableName + "/" + unMappedCols)
        }
        columnMap
    }

    private def convertType(schema: StructType): Type = {
        val columns: List[Column] = new ArrayList[Column]()

        for (field <- schema.fields) {
            val dataType: DataType = field.dataType
            val columnName: String = field.name
            var classType: Class[_] = null
            val colProps: ArrayList[String] = new ArrayList[String]()

            if (field.nullable) {
                colProps.add("nullable")
            }

            if (dataType == DataTypes.StringType) {
                classType = classOf[String]
            } else if (dataType == DataTypes.TimestampType) {
                classType = classOf[java.lang.Long]
                colProps.add(ColumnProperty.TIMESTAMP)
            } else if (dataType == DataTypes.DateType) {
                classType = classOf[java.lang.Long]
                colProps.add(ColumnProperty.TIMESTAMP)
            } else if (dataType == DataTypes.DoubleType) {
                classType = classOf[java.lang.Double]
            } else if (dataType == DataTypes.FloatType) {
                classType = classOf[java.lang.Float]
            } else if (dataType == DataTypes.IntegerType) {
                classType = classOf[java.lang.Integer]
            } else if (dataType == DataTypes.ShortType) {
                classType = classOf[java.lang.Integer]
                colProps.add(ColumnProperty.INT16)
            } else if (dataType == DataTypes.ByteType) {
                classType = classOf[java.lang.Integer]
                colProps.add(ColumnProperty.INT8)
            } else if (dataType == DataTypes.BooleanType) {
                classType = classOf[java.lang.Integer]
                colProps.add(ColumnProperty.INT8)
            } else if (dataType == DataTypes.LongType) {
                classType = classOf[java.lang.Long]
            } else if (dataType.isInstanceOf[DecimalType]) {
                // HACK: this may not fit in a long so we will use string
                classType = classOf[java.lang.String]
            } else {
                throw new Exception(
                    String.format("Column %s: Could not map type: %s (%s)",
                        columnName, dataType.toString, dataType.getClass.getCanonicalName))
            }

            columns.add(new Column(columnName, classType, colProps))
        }

        if (columns.isEmpty) {
            throw new Exception("Schema has no fields.")
        }

        new Type(columns)
    }

    private def validConversion(sourceDT: Class[_], destDT: Class[_]): Boolean = {
        if (destDT == sourceDT) {
            // same types
            true
        } else if (sourceDT == classOf[java.lang.Integer] && destDT == classOf[java.lang.Long]) {
            // widening conversion
            true
        } else if (sourceDT == classOf[java.lang.Float] && destDT == classOf[java.lang.Double]) {
            // widening conversion
            true
        } else if (sourceDT == classOf[java.lang.Boolean] && destDT == classOf[java.lang.Integer]) {
            // boolean to numeric conversion
            true
        } else if (classOf[java.util.Date].isAssignableFrom(sourceDT) && destDT == classOf[java.lang.Long]) {
            // timestamp conversion
            true
        } else {
          false
        }
    }
}
