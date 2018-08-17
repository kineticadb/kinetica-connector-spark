package com.kinetica.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.sql.functions.{ column, count, sum, when }

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
            count("*").as("TotalFlights"),
            sum("Diverted").as("TotalDiverted"),
            sum("Cancelled").as("TotalCancelled")
        ).
        orderBy("DayOfWeek").
        select(
           when(df("DayOfWeek") === 1, "Monday").
           when(df("DayOfWeek") === 2, "Tuesday").
           when(df("DayOfWeek") === 3, "Wednesday").
           when(df("DayOfWeek") === 4, "Thursday").
           when(df("DayOfWeek") === 5, "Friday").
           when(df("DayOfWeek") === 6, "Saturday").
           when(df("DayOfWeek") === 7, "Sunday").alias("DayOfWeek"),
           column("TotalFlights"),
           column("TotalDiverted"),
           column("TotalCancelled")
        ).
        show()

    println("Kinetica egress to Spark test finished.")

    System.exit(0)
}
