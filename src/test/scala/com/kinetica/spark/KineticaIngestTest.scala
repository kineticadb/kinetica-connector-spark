package com.kinetica.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SaveMode, SparkSession }

/*
 * This is a representative command to start this job. 
 * ./spark-submit 
 * --master "spark://172.31.70.10:7077" 
 * --class "com.kinetica.spark.KineticaIngestTest" 
 * --jars "<sandbox>/target/spark-kinetica-6.2.1-tests.jar" 
 * <sandbox>/target/spark-2.2.1-kinetica-6.2.1-connector-jar-with-dependencies.jar 
 * <somedir>/data/2008.csv
 */

object KineticaIngestTest extends App {

    println("Application Kinetica Spark started...")

    System.setProperty("spark.sql.warehouse.dir", "file:///C:/1SPARK/spark-warehouse");
    System.setProperty("hadoop.home.dir", "c:/1SPARK/")

    val conf = new SparkConf().setAppName("spark-custom-datasource")
    conf.set("spark.driver.userClassPathFirst" , "true");
    println("Conf created...")
    val spark = SparkSession.builder().config(conf).master("local").getOrCreate()
    
    if( args.length != 3 ) {
        println(" 3 params needed - InputFile KineticaTableName kineticaIp")
        println(" Aborting test")
        System.exit(-1);
    }

    val file = args(0)
    val tableName = args(1)
    val kineticaIp = args(2)
    
    val sqlContext = spark.sqlContext
    println("Conf values set...")
    
    sqlContext.udf.register("toInt", (str: String) => str.toInt)
    println("Udf toInt set...")
    
    /*
    var userDF = sqlContext.read.format("com.databricks.spark.csv")
        .option("delimiter", "|").option("header", "false").load(s"${dataDir}/u.user")
    userDF.registerTempTable("user")
    userDF = sqlContext.sql("select _c0 as user_id,toInt(_c1) as age, _c2 as gender, _c3 as occupation, _c4 as zip_code from user")
    println("UserDF done...")
    */
    
    val userDF = spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ",")
            .csv(file);
    
    println("printing schema...")
    userDF.printSchema()
    
    // Tested with createtable true and false......createtable=true will take way more time since 
    var writeToKineticaOpts = Map("database.url" -> s"http://${kineticaIp}:9191", "table.name" -> tableName,
            "table.is_replicated" -> "false", "ingester.ip_regex" -> "", "ingester.batch_size" -> "10000", "table.update_on_existing_pk" -> "true",
            "table.map_columns_by_name" -> "false", "ingester.num_threads" -> "4", "table.create" -> "true",
            "database.jdbc_url" -> s"jdbc:simba://${kineticaIp}:9292;URL=http://${kineticaIp}:9191;ParentSet=MASTER",
            "database.username" -> "", "database.password" -> "");
    println("writeToKineticaOpts set...")
    
    println("Starting Kinetica write...")
    userDF.write.format("com.kinetica.spark").options(writeToKineticaOpts).save()

    println("Ingest done....")
    System.exit(0);

}