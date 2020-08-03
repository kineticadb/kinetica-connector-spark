package com.kinetica.spark.datasourcev2

import com.kinetica.spark.LoaderParams
import com.kinetica.spark.util.ConfigurationConstants._
import com.kinetica.spark.util._
import com.kinetica.spark.util.table._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import scala.collection.JavaConverters._


class KineticaDataSourceWriter (schema: StructType, options: DataSourceOptions)
  extends DataSourceWriter
    with LazyLogging {

    // Parse the options (need a Scala immutable map)--need for table creation
    val conf: LoaderParams = new LoaderParams(None, options.asMap().asScala.toMap );

    // Ingestion related utility class
    val ingestionUtils = new KineticaSparkDFManager( conf );

    setup( schema );

    private def setup( schema: StructType ): Unit = {

        logger.info("Executing spark ingest ingest_analysis = {}", conf.isDryRun());

        // We can't proceed if the schema is empty
        if ( schema.isEmpty ) {
            logger.warn("The dataframe has no schema; skipping ingestion");
            return;
        }

        if (conf.isCreateTable && conf.isAlterTable) {
            throw new RuntimeException("Create table and alter table option set to true. Only one must be set to true ");
        }

        if( conf.hasTable() ) {
            if( conf.truncateTable ) {
                logger.info("Truncating/Creating table " + conf.getTablename);
                try {
                    SparkKineticaTableUtil.truncateTable(None, conf);
                } catch {
                    case e: Throwable => throw new RuntimeException("Failed with errors ", e);
                }
            }
        } else if (conf.isCreateTable) {
            if ( !conf.hasSchema ) {
                logger.info("Creating schema " + conf.getSchemaname +
                    " for table " + conf.getTablename);
                conf.createSchema;
            }

            logger.info("Creating table " + conf.getTablename);
            try {
                SparkKineticaTableUtil.createTable(None, Option.apply( schema ), conf);
            } catch {
                case e: Throwable => throw new RuntimeException("Failed with errors ", e);
            }
        }

        if (conf.isTableReplicated) {
            logger.info("Table is replicated");
        } else {
            logger.info("Table is not replicated");
        }

        // Note: Will NOT be altering table for datasource v2!!
        // ====================================================
        // logger.info("isAlterTable: " + conf.isAlterTable);
        // if (conf.isAlterTable) {
        //     logger.info("Altering table " + conf.getTablename());
        //     try {
        //         logger.debug("Alter table");
        //         SparkKineticaTableUtil.AlterTable(df, conf);

        //         //reset table type as alter table changed avro
        //         logger.debug("Get Kinetica Table Type");
        //         KineticaSparkDFManager.setType(conf);
        //         logger.debug("Set LoaderParms Table Type");
        //         conf.setTableType(KineticaSparkDFManager.getType(conf));
        //     } catch {
        //         case e: Throwable => throw new RuntimeException("Failed with errors ", e);
        //     }
        // }

        // if( !conf.dryRun ) {
        //     logger.info("Map and Write to Kinetica");
        //     KineticaSparkDFManager.KineticaMapWriter(sparkSession.sparkContext, conf);
        // } else {
        //     logger.info("@@@@@@@@@@ Execution was a dry-run. Look in the log for your schema and derived string column max lengths.");
        // }
    }   // end setup

    override def createWriterFactory(): DataWriterFactory[Row] = {

        // Need to convert options to a Scala map 'cause spark's closure complains about
        // its own DataSourceOptions (not serializable)
        new KineticaDataWriterFactory( schema, options.asMap().asScala.toMap )
    }

    override def commit(messages: Array[WriterCommitMessage]) = {
    }

    override def abort(messages: Array[WriterCommitMessage]) = {
    }

}   // end class KineticaDataSourceWriter




