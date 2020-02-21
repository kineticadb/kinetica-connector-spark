package com.kinetica.spark.datasourcev2

import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.sources.v2.ReadSupport
import org.apache.spark.sql.sources.v2.WriteSupport
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import java.util.Optional


import scala.collection.JavaConverters._
    
/*
 * This is the default datasource class which creates and returns the KineticaRelation class doing the actual save/fetch.
 */
class DefaultSource
    extends DataSourceV2
            with ReadSupport
            with WriteSupport {

    // DataSource v2 path
    override def createReader(options: DataSourceOptions): DataSourceReader = new KineticaDataSourceReader(options)
    
    // DataSource v2 path
    override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {

        Optional.of( new KineticaDataSourceWriter(schema, options) )
    }

}
