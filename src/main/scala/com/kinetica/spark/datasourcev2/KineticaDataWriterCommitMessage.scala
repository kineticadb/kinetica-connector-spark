package com.kinetica.spark.datasourcev2

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage



/**
 * Upon successful write commits, create a message describing what the
 * KineticaDataWriter did.
 *
 */
class KineticaDataWriterCommitMessage (partitionId: Int, taskId:Long, epochId: Long, tableName: String,
                                       numTotalRows: Long, numConvertedRows: Long, numFailedConversion: Long)
    extends WriterCommitMessage
            with LazyLogging {

    val message: String = s"Writer for table '$tableName' partition $partitionId (task #$taskId, epoch #$epochId) tried to parse $numTotalRows, succeeded with $numConvertedRows and failed with $numFailedConversion"
}  // end class KineticaDataWriterCommitMessage



