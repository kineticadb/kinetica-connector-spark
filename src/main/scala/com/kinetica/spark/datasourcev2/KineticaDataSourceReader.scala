package com.kinetica.spark.datasourcev2;

import com.kinetica.spark.egressutil._;

import com.kinetica.spark.LoaderParams;
import com.kinetica.spark.util.ConfigurationConstants._;
import com.kinetica.spark.egressutil.KineticaSchema;
import com.gpudb._;
import com.typesafe.scalalogging.LazyLogging;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters._;




class KineticaDataSourceReader (options: DataSourceOptions)
    extends DataSourceReader
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with LazyLogging {

    // Parse the options (need a Scala immutable map)--need for table creation
    val conf: LoaderParams = new LoaderParams(None, options.asMap().asScala.toMap );

    // Get some parameters
    val tableName = if (conf.getTablename != null ) conf.getTablename else sys.error("Option 'table.name' not specified");


    // Get the schema of the table; throw if it doesn't exist
    lazy val tableSchema: StructType = {
        val url = if (conf.getJdbcURL != null ) conf.getJdbcURL   else sys.error("Option 'database.jdbc_url' not specified");
        val throwIfNotExists : Boolean = true;
        KineticaSchema.getSparkSqlSchema(url, conf, tableName, throwIfNotExists).get;
    }

    override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
        val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]
        factoryList.add(new KineticaInputPartition(conf, tableSchema, pushedFilters, requiredSchema) )
        factoryList
    }

    /**
     * Returns the most applicable schema for the data--either derived from
     * the filters pushed down, or if no filter is given, the table's schema.
     */
    override def readSchema(): StructType = {
        if ( requiredSchema != null ) {
            return requiredSchema;
        } else {
            return tableSchema;
        }
    }

    /* For supporting SupportsPushDownFilters */
    var pushedFilters = Array[Filter]()

    override def pushFilters(filters: Array[Filter]): Array[Filter] = {
      pushedFilters = filters
      pushedFilters
    }

    /* For supporting SupportsPushDownRequiredColumns */

    private var requiredSchema: StructType = null;

    override def pruneColumns(schema: StructType): Unit = {
      requiredSchema = schema
    }

}   // end class KineticaDataSourceReader
