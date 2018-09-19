package com.kinetica.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode

object KineticaIngestTest extends App {

    println("Spark ingest to Kinetica test started...")

    if( args.length < 2 ) {
        println("Parameters:  <CSVFile> <KineticaHostName/IP> [<Username> <Password>]")
        println(" Aborting test")
        System.exit(-1)
    }

    val file = args(0)
    val host = args(1)
    val username = if (args.length > 2) args(2) else ""
    val password = if (args.length > 3) args(3) else ""

    val url = s"http://${host}:9191"

    /*
    val conf = new SparkConf().setAppName("spark-custom-datasource")
    conf.set("spark.driver.userClassPathFirst" , "true")
    val spark = SparkSession.builder().config(conf).getOrCreate()
	*/
    
    val conf = new SparkConf().setAppName(classOf[SparkKineticaDriver].getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ",")
            .load(file)
    
    println("printing schema...")
    df.printSchema()
    
    var kineticaOptions = Map(
        "database.url" -> url,
        "database.jdbc_url" -> s"jdbc:simba://${host}:9292;URL=${url};ParentSet=MASTER",
        "database.username" -> username,
        "database.password" -> password,
        "table.name" -> "airline",
        "table.create" -> "true",
        "table.truncate" -> "true",
        "table.is_replicated" -> "false",
        "table.update_on_existing_pk" -> "true",
        "table.map_columns_by_name" -> "false",
        "ingester.ip_regex" -> "",
        "ingester.batch_size" -> "10000",
        "ingester.num_threads" -> "4"
    )
    
    df.write.format("com.kinetica.spark").options(kineticaOptions).save()

    println("Spark ingest to Kinetica test finished.")
    
    /*
    spark.close()
    spark.stop()
    */
    System.exit(0);
}
