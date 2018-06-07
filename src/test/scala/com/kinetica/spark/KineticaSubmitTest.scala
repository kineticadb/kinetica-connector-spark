package com.kinetica.spark

import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import com.databricks.spark.avro._

class KineticaSubmitTest extends FunSuite with LazyLogging {

    test("Submit CSV file") {
        logger.info("starting CSV test")
        val args = Array("src/test/resources/csv-test.properties")
        val loaderJob: SparkKineticaDriver = new SparkKineticaDriver(args)
        val sess = SparkSession.builder()
            .appName(classOf[SparkKineticaDriver].getSimpleName)
            .master("local")
            .enableHiveSupport()
            .getOrCreate
        loaderJob.start(sess)
        assert(true)
    }

    test("Submit AVRO file") {
        logger.info("starting AVRO test")
        val args = Array("src/test/resources/avro-test.properties")
        val loaderJob: SparkKineticaDriver = new SparkKineticaDriver(args)
        val sess = SparkSession.builder()
            .appName(classOf[SparkKineticaDriver].getSimpleName)
            .master("local")
            .enableHiveSupport()
            .getOrCreate
        loaderJob.start(sess)
        assert(true)
    }

    // table to read from
    private final val TableName = "avro_test"

    // config options
    private final val KineticaOptions = Map(
        "kinetica-jdbcurl" -> "jdbc:simba://localhost:9292",
        "kinetica-username" -> "",
        "kinetica-password" -> "",
        "kinetica-desttablename" -> TableName,
        "connector-numparitions" -> "4")

    test("Egress CSV file") {
        logger.info("Starting CSV egress test")

        val spark = SparkSession.builder()
            .appName(classOf[SparkKineticaDriver].getSimpleName)
            .master("local")
            .getOrCreate()

        val tableDF = spark.read.format("com.kinetica.spark")
            .options(KineticaOptions).load()

        logger.info("Writing lines: {}", tableDF.count())
        tableDF.printSchema()

        tableDF.coalesce(1).write
            .mode("overwrite")
            .option("header", "true")
            .csv("output_csv")
    }

    test("Egress AVRO file") {
        logger.info("Starting AVRO egress test")

        val spark = SparkSession.builder()
            .appName(classOf[SparkKineticaDriver].getSimpleName)
            .master("local")
            .getOrCreate()

        val tableDF = spark.read.format("com.kinetica.spark")
            .options(KineticaOptions).load()

        logger.info("Writing lines: {}", tableDF.count())
        tableDF.printSchema()

        tableDF.coalesce(1).write
            .mode("overwrite")
            .option("header", "true")
            .avro("output_avro")
    }

    test("Ingest CSV file") {
        logger.info("Starting CSV ingest test")

        val spark = SparkSession.builder()
            .appName(classOf[SparkKineticaDriver].getSimpleName)
            .master("local")
            .getOrCreate()

        val tableDF = spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("scripts/data/flights.csv")

        logger.info("Writing lines: {}", tableDF.count())
        tableDF.printSchema()

        var writeToKineticaOpts = Map(
             "kinetica-url" -> "http://localhost:9191",
             "kinetica-desttablename" -> "flights",
             "kinetica-replicatedtable" -> "false",
             "kinetica-maptoschema" -> "false",
             "kinetica-createtable" -> "true",
             "kinetica-jdbcurl" -> "jdbc:simba://localhost:9292",
             "kinetica-username" -> "",
             "kinetica-multihead" -> "false",
             "kinetica-password" -> "");

        tableDF.write.format("com.kinetica.spark").options(writeToKineticaOpts).save()
    }
}
