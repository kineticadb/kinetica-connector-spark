package com.kinetica.spark.datasourcev2;

import com.kinetica.spark.LoaderParams;
import com.kinetica.spark.util.ConfigurationConstants._;
import com.typesafe.scalalogging.LazyLogging;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import scala.collection.JavaConverters._;
import org.apache.spark.sql.sources.Filter;

class KineticaInputPartition (
    val conf: LoaderParams,
    val tableSchema: StructType,
    val pushedFilters: Array[Filter],
    val requiredSchema: StructType)
    extends InputPartition[InternalRow]
    with LazyLogging {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
        new KineticaPartitionReader(conf, tableSchema, pushedFilters, requiredSchema)
  }

  // override def preferredLocations(): Array[String] = Array(kineticaShard.hostname)

}
