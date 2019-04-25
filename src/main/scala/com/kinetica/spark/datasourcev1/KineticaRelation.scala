package com.kinetica.spark.datasourcev1;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;
import com.typesafe.scalalogging.LazyLogging;
import org.apache.spark.SparkFirehoseListener;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.{ SparkListenerApplicationEnd, SparkListenerEvent };
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.sources._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.{ DataFrame, Row, SQLContext, SparkSession };
import scala.collection.JavaConverters._;
import scala.collection.breakOut;
import scala.collection.mutable.ListBuffer;
import scala.reflect.runtime.universe._;
import scala.util.control.Breaks._;
import java.util.Properties;

import com.kinetica.spark.egressutil._;
import com.kinetica.spark.loader._;
import com.kinetica.spark.LoaderParams;
import com.kinetica.spark.util.ConfigurationConstants._;
import com.kinetica.spark.util._;
import com.kinetica.spark.util.table._;



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

    logger.debug("*********************** KR:Constructor1");

    override val sqlContext: SQLContext = sparkSession.sqlContext;

    def this(parameters: Map[String, String], sparkSession: SparkSession) {
        this(parameters, None, sparkSession)
        logger.debug("*********************** KR:Constructor2")
    }

    // Parse the user given parameters
    val properties = new Properties();
    parameters.foreach { case (k, v) => properties.setProperty(k, v) };
    val conf: LoaderParams = new LoaderParams( Option.apply(sparkSession.sparkContext), parameters);

    // Save some user given parameters for use
    val url = conf.getJdbcURL;
    val tableName = conf.getTablename;

    // Needed only for egress
    lazy val tableSchema: Option[StructType] = {
        logger.debug("*********************** KR:tableSchema");
        val throwIfNotExists : Boolean = false;
        KineticaSchema.getSparkSqlSchema(url, conf, tableName, throwIfNotExists );
    }

    // Member of BaseRelation, so need to keep it around
    override def schema: StructType = tableSchema.get

    override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

    override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

        logger.debug( s"KineticaRelation::buildScan(): Got required columns: '${requiredColumns.mkString(", ")}'" )

        val numPartitions = conf.getNumPartitions;
                
        if (requiredColumns.isEmpty) {
            emptyRowRDD(filters, url, tableName, numPartitions)
        } else {
            val parts = com.kinetica.spark.egressutil.KineticaInputFormat.getDataSlicePartition(
                           com.kinetica.spark.egressutil.KineticaJdbcUtils.getConnector(url, conf)(),
                           numPartitions.toInt, tableName, filters);
            new KineticaRDD(
                sqlContext.sparkContext,
                KineticaJdbcUtils.getConnector(url, conf),
                KineticaSchema.pruneSchema( tableSchema, requiredColumns ),
                tableName,
                requiredColumns,
                filters,
                parts,
                properties,
                conf);
        }
    }

    override def insert(df: DataFrame, dummy: Boolean): Unit = {
        logger.debug("*********************** KR:insert")
        logger.info(" DF schema is -> " + df.schema.treeString);
        val loaderPath: Boolean = parameters.get(ConfigurationConstants.LOADERCODEPATH).getOrElse("false").toBoolean
        logger.debug("*********************** loaderPath var is " + loaderPath)
        val jdbcurl: Option[String] = parameters.get(ConfigurationConstants.KINETICA_JDBCURL_PARAM)
        logger.debug("*********************** jdbcurlPresent var is " + jdbcurl)

        if (loaderPath && jdbcurl.isEmpty) {
            logger.info("*********************** loading loader way with config file....")
            insertLoaderWay(df, dummy)
        } else {
            logger.info("*********************** loading connector way....")
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

        logger.info("Executing spark ingest ingest_analysis = {}", conf.isDryRun());

        if (df.rdd.isEmpty()) {
            throw new KineticaException("Dataframe/Dataset is empty, try again");
        }

        if (conf.isCreateTable && conf.isAlterTable) {
            throw new KineticaException("Create table and alter table option set to true. Only one must be set to true ");
        }

        // The dataframe being ingested may have nested structs, arrays and maps. If the user wants to flatten the schema...
        val dfSource = if (conf.isFlattenSourceSchema) {
            logger.info("Flattening/Exploding source schema....")
            val almostFlat = Flatten.flatten_all(df)
            val jsonString = almostFlat.toJSON
            sqlContext.read.json(jsonString)
        } else {
            logger.info("NO Flattening/Exploding of original source schema....")
            df
        }

        if (conf.isDryRun()) {
            try {
                SparkKineticaTableUtil.createTable( Option.apply(dfSource), None, conf );
            } catch {
                case e: Throwable => throw new RuntimeException("Failed with errors ", e);
            }
            logger.info("****** Execution was a dry-run. Create table DLL and derived string columns max lengths available as log statements.");
        } else {
            if (SparkKineticaTableUtil.tableExists(conf)) {
                if (conf.truncateTable) {
                    logger.info("Truncating/Creating table " + conf.getTablename);
                    try {
                        SparkKineticaTableUtil.truncateTable(Option.apply(dfSource), conf);
                    } catch {
                        case e: Throwable => throw new RuntimeException("Failed with errors ", e);
                    }
                }
            } else if (conf.isCreateTable) {
                try {
                    SparkKineticaTableUtil.createTable( Option.apply(dfSource), None, conf );
                } catch {
                    case e: Throwable => throw new RuntimeException("Failed with errors ", e);
                }
            }
            logger.debug("Get Kinetica Table Type");
            KineticaSparkDFManager.setType(conf);

            logger.debug("Set LoaderParms Table Type");
            conf.setTableType(KineticaSparkDFManager.getType(conf));

            logger.debug("Set DataFrame");
            KineticaSparkDFManager.setDf(dfSource);

            if (conf.isTableReplicated) {
                logger.info("Table is replicated");
            } else {
                logger.info("Table is not replicated");
            }

            logger.info("Map and Write to Kinetica...");
            KineticaSparkDFManager.KineticaMapWriter(sparkSession.sparkContext, conf);
            logger.info("Map and Write to Kinetica done.");
            // Lets try and print the accumulators
            logger.info(" Total rows = " + conf.totalRows.value);
            logger.info(" Converted rows = " + conf.convertedRows.value);
            logger.info(" Columns failed conversion = " + conf.failedConversion.value);
        } 
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
