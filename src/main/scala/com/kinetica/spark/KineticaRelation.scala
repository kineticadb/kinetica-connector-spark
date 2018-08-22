package com.kinetica.spark

import java.io.IOException
import java.util.UUID
import java.util.regex.Pattern
import com.kinetica.spark.util.ConfigurationConstants._
import com.kinetica.spark.util._
import com.kinetica.spark.util.table._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkFirehoseListener
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ SparkListenerApplicationEnd, SparkListenerEvent }
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext, SparkSession }
import scala.collection.JavaConverters._
import scala.collection.breakOut
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
import scala.util.control.Breaks._
import com.kinetica.spark.egressutil._
import java.util.Properties

import com.kinetica.spark.loader._

class KineticaRelation(
    val parameters: Map[String, String],
    val dataFrame: Option[DataFrame],
    @transient val sparkSession: SparkSession)
    extends BaseRelation
    with Serializable
    with TableScan
    with PrunedFilteredScan
    with InsertableRelation
    with LazyLogging {

    logger.debug("*********************** KR:Constructor1")

    override val sqlContext: SQLContext = sparkSession.sqlContext

    def this(parameters: Map[String, String], sparkSession: SparkSession) {
        this(parameters, None, sparkSession)
        logger.debug("*********************** KR:Constructor2")
    }

    val properties = new Properties()
    parameters.foreach { case (k, v) => properties.setProperty(k, v) }
    val conf: LoaderParams = new LoaderParams(sparkSession.sparkContext, parameters)

    lazy val querySchema: StructType = {
        logger.debug("*********************** KR:querySchema")
        val url = parameters.getOrElse(KINETICA_JDBCURL_PARAM, sys.error("Option 'database.jdbc_url' not specified"))
        val table = parameters.getOrElse(KINETICA_TABLENAME_PARAM, sys.error("Option 'table.name' not specified"))
        KineticaSchema.getSparkSqlSchema(url, conf, table)
    }

    override def schema: StructType = querySchema

    override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

    override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

        logger.debug("*********************** KR:BS ")

        val conf: LoaderParams = new LoaderParams(sparkSession.sparkContext, parameters)
        val url = parameters.getOrElse(KINETICA_JDBCURL_PARAM, sys.error("Option 'database.jdbc_url' not specified"))
        val table = parameters.getOrElse(KINETICA_TABLENAME_PARAM, sys.error("Option 'table.name' not specified"))
        val numPartitions = parameters.getOrElse(CONNECTOR_NUMPARTITIONS_PARAM, "4").toInt

        if (requiredColumns.isEmpty) {
            emptyRowRDD(filters, url, table, numPartitions)
        } else {
            val parts = com.kinetica.spark.egressutil.KineticaInputFormat.getDataSlicePartition(
                com.kinetica.spark.egressutil.KineticaJdbcUtils.getConnector(url, conf)(), numPartitions.toInt, table, filters)
            new KineticaRDD(
                sqlContext.sparkContext,
                KineticaJdbcUtils.getConnector(url, conf),
                KineticaSchema.pruneSchema(schema, requiredColumns),
                table,
                requiredColumns,
                filters,
                parts,
                properties)
        }
    }

    override def insert(df: DataFrame, dummy: Boolean): Unit = {
        logger.debug("*********************** KR:insert")
        val loaderPath: Boolean = parameters.get(ConfigurationConstants.LOADERCODEPATH).getOrElse("false").toBoolean
        logger.debug("*********************** loaderPath var is " + loaderPath)
        val jdbcurl: Option[String] = parameters.get(ConfigurationConstants.KINETICA_JDBCURL_PARAM)
        logger.debug("*********************** jdbcurlPresent var is " + jdbcurl)
        
        if (loaderPath && jdbcurl.isEmpty) {
            logger.debug("*********************** loading loader way with config file....")
            insertLoaderWay(df, dummy)
        } else {
            logger.debug("*********************** loading connector way....")
            insertConnectorWay(df, dummy)
        }
    }

    private def insertLoaderWay(df: DataFrame, dummy: Boolean): Unit = {
        val loaderConfig = new LoaderConfiguration(sparkSession.sparkContext, parameters)
        val mapper: SchemaManager = new SchemaManager(loaderConfig)
        //val dfRenamed = mapper.adjustSourceSchema(df)
        //val columnMap: java.util.HashMap[Integer, Integer] = mapper.setupSchema(loaderConfig, dfRenamed.schema)
        val columnMap: java.util.HashMap[Integer, Integer] = mapper.setupSchema(loaderConfig, df.schema)
        val kineticaFunction = new KineticaLoaderFunction(loaderConfig, columnMap)
        df.foreachPartition(kineticaFunction)
    }

    private def insertConnectorWay(df: DataFrame, dummy: Boolean): Unit = {

        /* FOR DEBUGGING - KEEP COMMENTED OUT
        val kineticaUrl = conf.getKineticaURL();
        val dfSchema = df.schema
        println("KineticaInsert insert executing for host " + kineticaUrl)
        dfSchema.fields.foreach(f => {
            println("Schema field is " + f.name)
            println("Schema field dataType is " + f.dataType.typeName)
        })
		*/

        if (df.rdd.isEmpty()) {
            throw new KineticaException("Dataframe/Dataset is empty, try again");
        }

        if (conf.isCreateTable && conf.isAlterTable) {
            throw new KineticaException("Create table and alter table option set to true. Only one must be set to true ");
        }

        if( SparkKineticaTableUtil.tableExists(conf) ) {
          if( conf.truncateTable ) {
              logger.info("Truncating/Creating table " + conf.getTablename);
              try {
                  SparkKineticaTableUtil.truncateTable(df, conf);
              } catch {
                  case e: Throwable => throw new RuntimeException("Failed with errors ", e);
              }
          }
        } else if (conf.isCreateTable) {
            logger.info("Creating table " + conf.getTablename);
            try {
                SparkKineticaTableUtil.createTable(df, conf);
            } catch {
                case e: Throwable => throw new RuntimeException("Failed with errors ", e);
            }
        }

        logger.debug("Get Kinetica Table Type");
        KineticaSparkDFManager.setType(conf);

        logger.debug("Set LoaderParms Table Type");
        conf.setTableType(KineticaSparkDFManager.getType(conf));

        logger.debug("Set DataFrame");
        KineticaSparkDFManager.setDf(df);

        if (conf.isTableReplicated) {
            logger.info("Table is replicated");
        } else {
            logger.info("Table is not replicated");
        }

        logger.info("isAlterTable: " + conf.isAlterTable);
        if (conf.isAlterTable) {
            logger.info("Altering table " + conf.getTablename());
            try {
                logger.debug("Alter table");
                SparkKineticaTableUtil.AlterTable(df, conf);

                //reset table type as alter table changed avro
                logger.debug("Get Kinetica Table Type");
                KineticaSparkDFManager.setType(conf);
                logger.debug("Set LoaderParms Table Type");
                conf.setTableType(KineticaSparkDFManager.getType(conf));
            } catch {
                case e: Throwable => throw new RuntimeException("Failed with errors ", e);
            }
        }

        logger.info("Map and Write to Kinetica");
        KineticaSparkDFManager.KineticaMapWriter(sparkSession.sparkContext, conf);


        // Lets try and print the accumulators
        println(" Total rows = " + conf.totalRows.value)
        println(" Converted rows = " + conf.convertedRows.value)
        println(" Columns failed conversion = " + conf.failedConversion.value)
    }

    /**
     * In case of select count, actual data is not needed to flow through the network to form a RDD,
     * a RDD with empty rows of the expected count will be returned to be counted.
     * @param filters the filters to apply to get correct number of rows
     * @return
     */
    private def emptyRowRDD(filters: Array[Filter], url: String, table: String, numPartitions: Integer): RDD[Row] = {
        val numRows: Long = KineticaJdbcUtils.getCountWithFilter(url, conf, table, filters)
        val emptyRow = Row.empty
        sqlContext.sparkContext.parallelize(1L to numRows, numPartitions).map(_ => emptyRow)
    }
}

object KineticaRelation extends LazyLogging {

}

case class StreamField(name: String, dataType: DataType, alias: Option[String], hasReplace: Boolean = false)
case class StreamFields(collection: String, fields: ListBuffer[StreamField], metrics: ListBuffer[StreamField])
