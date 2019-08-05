package com.kinetica.spark.datasourcev2

import com.kinetica.spark.LoaderParams
import com.kinetica.spark.util.ConfigurationConstants._
import com.kinetica.spark.util._
import com.kinetica.spark.util.table._
import com.typesafe.scalalogging.LazyLogging
import com.gpudb.BulkInserter
import com.gpudb.GPUdb
import com.gpudb.GPUdbException
import com.gpudb.GenericRecord
import com.gpudb.Type
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.DataWriter
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.util.LongAccumulator
import scala.collection.JavaConverters._


/**
 * Writes the data to Kinetica.
 *
 */
class KineticaDataWriter (schema: StructType, options: Map[String, String], partitionId: Int, attemptNumber: Int)
    extends DataWriter[Row]
    with LazyLogging {


    // Parse the options (need a Scala immutable map)--each writer should have
    // its own configuration parameters (since it does more and save # inserted states)
    val conf: LoaderParams = new LoaderParams(None, options )

    // Kinetica table type (table should exist by now)
    private val tableType: Type = 
        try {
            Type.fromTable(conf.getGpudb, conf.getTablename);
        } catch {
            case e: GPUdbException => {
                logger.error("Could not create table type: ", e)
                throw e
            }
        }
    // Set it for the configuration param
    conf.setType( tableType )

    
    // The inserter for the table
    private val recordInserter: BulkInserter[GenericRecord] = 
        try {
            new KineticaBulkLoader( conf ).GetBulkInserter()
        } catch {
            case e: GPUdbException => {
                logger.error("Could not create bulk inserter: ", e)
                throw e
            }
        }    

    // Keeping some stats
    private val totalRows        = new LongAccumulator();
    private val convertedRows    = new LongAccumulator();
    private val failedConversion = new LongAccumulator();

    private val ingestionUtils = new KineticaSparkDFManager( conf );
        
        
    /**
     * Insert a row of record into Kinetica
     */
    def write(record: Row) = {
        // Keep track of how many records have been attempted for insertion
        totalRows.add( 1 );

        // Create the record
        val genericRecord: GenericRecord = new GenericRecord( tableType );
        var i: Int = 0;
        var isRecordGood = true;
        for (column <- tableType.getColumns.asScala) {
            try {
                var rtemp: Any = record.get({ i += 1; i - 1 })
                if ( conf.isMapToSchema ) {
                    rtemp = record.getAs(column.getName)
                }
                if( rtemp != null ) { // This means null value - nothing to do.
                    if (!ingestionUtils.putInGenericRecord(genericRecord, rtemp, column)) {
                        failedConversion.add( 1 );
                        isRecordGood = false;
                    }
                }
            } catch {
                case e: Exception => {
                //e.printStackTrace()
                    isRecordGood = false;
                    failedConversion.add( 1 );
                    logger.warn(s"Found non-matching column ${column.getName}; skipping record");
                    logger.debug(s"Found non-matching column ${column.getName}; skipping record; reason: ", e);
                    if ( conf.failOnError ) {
                        // Throw exception only for fail-fast mode
                        throw e;
                    }
                }
            }
        }

        if ( isRecordGood ) {
            // Insert the record into the queue only if it was successfully
            // converted
            recordInserter.insert( genericRecord );

            // Keep track of how many records have been inserted
            convertedRows.add( 1 );
        }
    }

    def commit(): WriterCommitMessage = {
        try {
            recordInserter.flush()
            new KineticaDataWriterCommitMessage( partitionId, attemptNumber, conf.getTablename,
                                                 totalRows.value, convertedRows.value, failedConversion.value );
        } catch {
            case e: GPUdbException => {
                logger.error(s"Error flushing records to Kinetica: '${e.getMessage()}'");
                logger.debug(s"Error flushing records to Kinetica; stacktrace for debugging: ", e);
                if ( conf.failOnError ) {
                    throw e;
                }
                else {
                    // Could not ingest any record!
                    new KineticaDataWriterCommitMessage( partitionId, attemptNumber, conf.getTablename,
                                                         totalRows.value, 0, totalRows.value );
                }
            }
        }
    }

    def abort() = {
    }


}  // end class KineticaDataWriter
