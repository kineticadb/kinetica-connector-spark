package com.kinetica.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SaveMode, SparkSession, functions }

object KineticaEgressTest extends App {

    println("Kinetica egress to Spark test started...")
    
    if( args.length < 1 ) {
        println("Parameters:  <KineticaHostName/IP> [<Username> <Password>]")
        println(" Aborting test")
        System.exit(-1)
    }

    val host = args(0)
    val username = if (args.length > 1) args(1) else ""
    val password = if (args.length > 2) args(2) else ""

    val url = s"http://${host}:9191"

    val conf = new SparkConf().setAppName("spark-custom-datasource")
    val spark = SparkSession.builder().config(conf).master("local").getOrCreate()
    val sqlContext = spark.sqlContext

    
    val kineticaOptions = Map(
        "database.jdbc_url" -> s"jdbc:simba://${host}:9292;URL=${url};ParentSet=MASTER",
        "database.username" -> username,
        "database.password" -> password,
        "table.name" -> "airline",
        "spark.num_partitions" -> "80"
    )
        
    val df = sqlContext.read.format("com.kinetica.spark").options(kineticaOptions).load().filter("Month = 7")
    
    df.write.format("csv").mode("overwrite").save("2008.july")

    df.
        groupBy("DayOfWeek").
        agg(
            functions.count("*").as("TotalFlights"),
            functions.sum("Diverted").as("TotalDiverted"),
            functions.sum("Cancelled").as("TotalCancelled")
        ).
        orderBy("DayOfWeek").
        show()

    println("Kinetica egress to Spark test finished.")

    System.exit(0)
}
