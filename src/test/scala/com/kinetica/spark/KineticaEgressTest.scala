package com.kinetica.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SaveMode, SparkSession }

object KineticaEgressTest extends App {

    println("Application Kinetica to Spark started...")
    
    if( args.length != 1 ) {
        println(" 1 params needed - <KineticaHostName/IP>")
        println(" Aborting test")
        System.exit(-1);
    }

    System.setProperty("spark.sql.warehouse.dir", "file:///C:/1SPARK/spark-warehouse");
    System.setProperty("hadoop.home.dir", "c:/1SPARK/")

    val conf = new SparkConf().setAppName("spark-custom-datasource")
    println("Conf created...")
    val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

    val host = args(0)
    val URL = s"http://${host}:9191"
    
    val kineticaOptions = Map(
        "database.jdbc_url" -> s"jdbc:simba://${host}:9292;URL=${URL};ParentSet=MASTER",
        "database.username" -> "",
        "database.password" -> "",
        "table.name" -> "airline",
        "spark.num_partitions" -> "80")
        
    val sqlContext = spark.sqlContext
    val productdf = sqlContext.read.format("com.kinetica.spark").options(kineticaOptions).load()
    
    productdf.printSchema
    
    val df = productdf.filter("DayOfMonth >= 12")
    
    //println(df.count())

    println(df.groupBy("FlightNum").sum("DayOfWeek").orderBy("FlightNum").show(20000))
    
    //df.write.format("csv").save("c:/1SPARK/data/dom12/")

    println("Egress completed....")
    System.exit(0);

}