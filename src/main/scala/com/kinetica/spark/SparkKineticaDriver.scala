package com.kinetica.spark

import java.io.File
import java.nio.file.Path
import java.nio.file.Paths

import com.kinetica.spark.loader.LoaderConfiguration
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.configuration.ConfigurationException
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object SparkKineticaDriver extends LazyLogging {

    logger.info("Version {}", getVersionString)

    def main(args: Array[String]): Unit = {
        System.setProperty("spark.sql.warehouse.dir", "file:///C:/1SPARK/spark-warehouse")
        System.setProperty("hadoop.home.dir", "c:/1SPARK/")
        if (args.length < 1) {
            throw new Exception("First argument must be a properties file.")
        }
        val loaderJob: SparkKineticaDriver = new SparkKineticaDriver(args)

        val sess = SparkSession.builder()
            .appName(classOf[SparkKineticaDriver].getSimpleName)
            .enableHiveSupport()
            .getOrCreate
        loaderJob.start(sess)

        sess.close()
        sess.stop()
    }

    private def getVersionString: String = {
        val thisPackage: Package = this.getClass.getPackage
        val thisTitle: String = thisPackage.getImplementationTitle
        val thisVersion: String = thisPackage.getImplementationVersion
        String.format("%s (build %s)", thisTitle, thisVersion)
    }
}

import com.kinetica.spark.util.ConfigurationConstants

class SparkKineticaDriver(args: Array[String]) extends LazyLogging {

    logger.debug(" ********** SparkKineticaDriver class main constructor ********** ")

    private val propertyConf: PropertiesConfiguration = parseArgs(args)

    var params: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()

    val propIt : java.util.Iterator[_] = propertyConf.getKeys()

    while (propIt.hasNext) {
        val key: String  = propIt.next.toString
        val param: String = propertyConf.getString(key)
        logger.debug("config: {} = {}", key, param)
        params += (key -> param)
    }

    // Dataframe is ready. Lets put a flag in the params so the datasource API can take
    // one of the 2 different paths from 2 original connectors.
    params += (ConfigurationConstants.LOADERCODEPATH -> "true")

    val immutableParams: Map[String, String] = params.map(kv => (kv._1,kv._2)).toMap
    var loaderConfig : LoaderConfiguration = _

    def start(sess: SparkSession): Unit = {

        loaderConfig = new LoaderConfiguration(sess.sparkContext,  immutableParams)

        logger.info("Starting job: {}", sess.conf.get("spark.app.name"))
        val inputDs: DataFrame = getDataset(sess)

        logger.info("Starting Kinetica write...")
        if ( loaderConfig.datasourceVersion == "v1" ) {
            logger.info("Using the Spark DataSource v1 API for loading")
            inputDs.write.format("com.kinetica.spark.datasourcev1").options(params).save()
        }
        else if ( loaderConfig.datasourceVersion == "v2" ) {
            logger.info("Using the Spark DataSource v2 API for loading")
            inputDs.write.format("com.kinetica.spark.datasourcev2").options(params).save()
        }
        else {
            val errorMsg: String = s"Must provide a valid value for parameter '${ConfigurationConstants.SPARK_DATASOURCE_VERSION}', if given.  Accepted values: 'v1', 'v2'; given '${loaderConfig.datasourceVersion}'"
            logger.error( errorMsg )
            throw new Exception( errorMsg )
        }
    }

    private def getDataset(sess: SparkSession): DataFrame = {

        val sqlFileName: String = loaderConfig.sqlFileName
        val dataPath: String = loaderConfig.dataPath
        var dataFormat: String = loaderConfig.dataFormat

        var inputDs: DataFrame = null
        val parentPath: Path = Paths.get(this.propertyConf.getFile.getParent)

        if (sqlFileName != null) {
            val sqlFile: File = parentPath.resolve(sqlFileName).toFile
            val sql: String = FileUtils.readFileToString(sqlFile)
            logger.info("Executing SQL: {}", sql)
            inputDs = sess.sql(sql)
        } else if (dataPath != null) {
            if (dataFormat == null) {
                throw new Exception("You must specify parameter 'source.data_format'")
            }

            if (dataFormat.equalsIgnoreCase("avro")) {
                dataFormat = "com.databricks.spark.avro"
            }

            logger.info("Attempting to load file as {}: {}", dataFormat, dataPath)
            val dfReader: DataFrameReader = sess.read.format(dataFormat)

            if(dataFormat.equalsIgnoreCase("csv") && loaderConfig.csvHeader) {
                dfReader.option("header", "true")
                dfReader.option("inferSchema", "true")
            } else if(dataFormat.equalsIgnoreCase("csv")) {
                dfReader.option("inferSchema", "true")
            }

            inputDs = dfReader.load(dataPath)
        } else {
            throw new Exception("You must set loader.sql-file or loader.data-file.")
        }

        if(loaderConfig.partitionRows > 0) {
            inputDs = repartition(inputDs)
        }

        inputDs
    }

    private def repartition(inputDs: DataFrame): DataFrame = {
        val origPartitions: Int = inputDs.javaRDD.getNumPartitions
        val numRows: Long = inputDs.count
        logger.info("Original dataset has <{}> rows and <{}> partitions.", numRows, origPartitions)

        val newPartitions: Int = (numRows / loaderConfig.partitionRows.toLong).toInt + 1
        logger.info("Repartitioning dataset to <{}> partitions.", newPartitions)
        inputDs.repartition(newPartitions)
    }

    private def parseArgs(args: Array[String]): PropertiesConfiguration = {
        val argList: java.util.List[String] = new java.util.ArrayList[String](java.util.Arrays.asList(args: _*))
        val propPath: String = argList.remove(0)
        val propFile: File = new File(propPath)
        logger.info("Reading properties from file: {}", propFile)

        val conf = new PropertiesConfiguration(propPath)

        val iter: java.util.Iterator[String] = argList.iterator()
        while (iter.hasNext) {
            var key: String = iter.next()
            if (!key.startsWith("--")) {
                throw new ConfigurationException("Config key is missing '--': " + key)
            }
            key = key.substring(2)
            if (!iter.hasNext) {
                throw new ConfigurationException(
                    "No value found for parameter: " + key)
            }
            val value: String = iter.next()
            logger.debug("commnd line: {} = {}", Array(key, value): _*)
            conf.setProperty(key, value)
        }
        conf
    }
}

