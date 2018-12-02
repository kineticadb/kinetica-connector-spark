package com.kinetica.spark.datasourcev2

import com.kinetica.spark.egressutil._

import com.kinetica.spark.LoaderParams
import com.kinetica.spark.util.ConfigurationConstants._
import com.kinetica.spark.egressutil.KineticaSchema
import com.gpudb._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownCatalystFilters
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import scala.collection.JavaConverters._




class KineticaDataSourceReader (options: DataSourceOptions)
    extends DataSourceReader
    with SupportsPushDownCatalystFilters
    with SupportsPushDownRequiredColumns
    with LazyLogging {

    // Parse the options (need a Scala immutable map)--need for table creation
    val conf: LoaderParams = new LoaderParams(None, options.asMap().asScala.toMap )

    // Get the schema of the table
    lazy val tableSchema: StructType = {
        logger.debug("*********************** KR:querySchema")
        val url   = if (conf.getJdbcURL   != null ) conf.getJdbcURL   else sys.error("Option 'database.jdbc_url' not specified")
        val table = if (conf.getTablename != null ) conf.getTablename else sys.error("Option 'table.name' not specified")
        KineticaSchema.getSparkSqlSchema(url, conf, table)
    }

    override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
     
        val factoryList = List( new KineticaDataReaderFactory(conf, tableSchema, pushedCatalystFilters, requiredSchema).asInstanceOf[DataReaderFactory[Row]] );
        factoryList.asJava;
    }
    
    private var schema: Option[StructType] = Option.apply( tableSchema )

    /**
     * Returns the table schema
     */
    override def readSchema(): StructType = {
        return tableSchema
    }

    /* SupportsPushDownCatalystFilters */
    var pushedCatalystFilters = Array[Expression]()
    
    override def pushCatalystFilters(filters: Array[Expression]): Array[Expression] = {
      pushedCatalystFilters = filters
      pushedCatalystFilters
    }

    /* SupportsPushDownRequiredColumns */
    
    private var requiredSchema: StructType = new StructType()
 
    override def pruneColumns(schema: StructType): Unit = {
      requiredSchema = schema
    }
    
}   // end class KineticaDataSourceReader

