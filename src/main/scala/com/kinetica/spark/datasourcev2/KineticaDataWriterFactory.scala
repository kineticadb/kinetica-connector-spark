package com.kinetica.spark.datasourcev2

import org.apache.spark.sql.sources.v2.writer.DataWriter
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType



/**
 * Create KineticaDataWriter instances and return them.
 *
 */
class KineticaDataWriterFactory (schema: StructType, options: Map[String, String])
    extends DataWriterFactory[Row] {

    override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
        new KineticaDataWriter( schema, options, partitionId, attemptNumber )
    }
}
