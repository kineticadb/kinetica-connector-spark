package com.kinetica.spark.datasourcev2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.DataWriter
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType



/**
 * Create KineticaDataWriter instances and return them.
 *
 */
class KineticaDataWriterFactory (schema: StructType, options: Map[String, String])
    extends DataWriterFactory[InternalRow] {

    override def createDataWriter(partitionId: Int, taskId:Long, epochId: Long ): DataWriter[InternalRow] = {
        new KineticaDataWriter( schema, options, partitionId, taskId, epochId )
    }
}
