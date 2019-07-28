package com.kinetica.spark.datasourcev2;

import com.kinetica.spark.LoaderParams;
import com.kinetica.spark.util.ConfigurationConstants._;
import com.typesafe.scalalogging.LazyLogging;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import scala.collection.JavaConverters._;

class KineticaDataReaderFactory (
    val conf: LoaderParams,
    val tableSchema: StructType,
    val pushedCatalystFilters: Array[Expression],
    val requiredSchema: StructType)
    extends DataReaderFactory[Row]
    with LazyLogging {
  
  override def createDataReader(): DataReader[Row] = {
        new KineticaDataReader(conf, tableSchema, pushedCatalystFilters, requiredSchema)
  }
  
  // override def preferredLocations(): Array[String] = Array(kineticaShard.hostname)
    
}
