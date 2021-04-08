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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.DataWriter
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.util.LongAccumulator
import scala.collection.JavaConverters._


/**
 * Writes the data to Kinetica.
 *
 */
class KineticaDataWriter (schema: StructType, options: Map[String, String], partitionId: Int, taskId:Long, epochId: Long)
    extends DataWriter[InternalRow]
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

    // Get an encoder for converting InternalRows to Rows
    val rowEncoder: ExpressionEncoder[Row] = RowEncoder.apply( schema ).resolveAndBind();

    // Keeping some stats
    private val totalRows        = new LongAccumulator();
    private val convertedRows    = new LongAccumulator();
    private val failedConversion = new LongAccumulator();

    private val ingestionUtils = new KineticaSparkDFManager( conf );


    /**
     * Insert a row of record into Kinetica
     */
    def write(record: InternalRow) = {
        // Keep track of how many records have been attempted for insertion
        totalRows.add( 1 );

        // Convert the spark InternalRow to Row for ease of processing
        val recordRow: Row = rowEncoder.fromRow( record );

        // We'll need a Java API-compatible object for insertion
        val genericRecord: GenericRecord = new GenericRecord( tableType );
        var i: Int = 0;
        var isRecordGood = true;

        // Add each column of the spark record to the Kinetica record
        for (column <- tableType.getColumns.asScala) {
            try {
                var columnIndex = i;

                // Check if  we need to map the columns by name instead of
                // index in in the type
                if ( conf.isMapToSchema ) {
                    columnIndex = schema.fieldIndex( column.getName );
                }

                // Get the column value by index
                var rtemp: Any = recordRow.get( columnIndex );

                // Set the column value in the destination
                if (!ingestionUtils.putInGenericRecord(genericRecord, rtemp, column)) {
                    // The column had an invalid or incompatible value.  Keep
                    // track that this record shouldn't be inserted
                    isRecordGood = false;

                    // Throw exception only for the fail-fast mode
                    if ( conf.failOnError ) {
                        val message = s"Failed to set value for column ${column.getName}";
                        logger.error( message );
                        throw new Exception( message );
                    } else {
                    }
                }


                // Increment the column index counter for the next loop
                i += 1;
            } catch {
                case e: Exception => {
                    isRecordGood = false;
                    val message = s"Failed to set value for column ${column.getName}";
                    logger.warn( message );
                    logger.debug(s"${message}; reason: ", e);
                    if ( conf.failOnError ) {
                        // Throw exception only for fail-fast mode
                        logger.error(s"${message}; reason: ", e);
                        // Since we'll throw, we need to increment the failed record
                        // tally here (otherwise, it will be done in the else block
                        // after this for loop.
                        failedConversion.add( 1 );
                        throw e;
                    } else {
                        logger.error(s"${message}; skipping record; reason: ", e);
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
        } else {
            // Keep track of how many records will not be inserted
            failedConversion.add( 1 );
        }
    }

    def commit(): WriterCommitMessage = {
        try {
            recordInserter.flush()
            new KineticaDataWriterCommitMessage( partitionId, taskId, epochId, conf.getTablename,
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
                    new KineticaDataWriterCommitMessage( partitionId, taskId, epochId, conf.getTablename,
                                                         totalRows.value, 0, totalRows.value );
                }
            }
        }
    }

    def abort() = {
    }


}  // end class KineticaDataWriter
