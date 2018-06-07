package com.kinetica.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SaveMode, SparkSession }

object KineticaEgressTest extends App {

    println("Application Spark to Kinetica started...")

    System.setProperty("spark.sql.warehouse.dir", "file:///C:/1SPARK/spark-warehouse");
    System.setProperty("hadoop.home.dir", "c:/1SPARK/")

    val conf = new SparkConf().setAppName("spark-custom-datasource")
    println("Conf created...")
    val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

    /* If we decide to write code.....*/
    val kineticaOptions = Map(
        "kinetica-jdbcurl" -> "jdbc:simba://172.31.70.13:9292;URL=http://172.31.70.13:9191;ParentSet=MASTER",
        "kinetica-username" -> "",
        "kinetica-password" -> "",
        "kinetica-desttablename" -> "ALLTYPE",//"airline"
        "connector-numparitions" -> "4")

    val sqlContext = spark.sqlContext
    val productdf = sqlContext.read.format("com.kinetica.spark").options(kineticaOptions).load()
    
    productdf.printSchema
    
    // FOR ALLTYPE
    val df = productdf.filter("CDOUBLE >= 10.5 AND CINT >= 11")
    
    //val df = productdf.filter("DayOfMonth >= 6")


    /* If we want to use the stock jdbc relation - method 1
    val df = spark.read.format("org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider").
        options(Map("url" -> "jdbc:simba://172.31.70.13:9292;URL=http://172.31.70.13:9191;ParentSet=MASTER",  
                "user" -> "", "dbtable"-> "movieuser",
                "password" -> "", "driver" -> "com.simba.client.core.jdbc4.SCJDBC4Driver")).load 
    */
    
    /* If we want to use the stock jdbc relation - method 2
    spark.sql("""CREATE TABLE movieuserspark USING org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider 
          OPTIONS (url 'jdbc:simba://172.31.70.13:9292;URL=http://172.31.70.13:9191;ParentSet=MASTER', 
          dbtable 'movieuser', user '', password '', driver 'com.simba.client.core.jdbc4.SCJDBC4Driver')""") 
          
    val df = spark.sql("select * from movieuserspark where _c2 = 'M'")          
    */      
          
    df.printSchema
    
    //println(df.count)
    
    println(df.groupBy("CSTRING").sum("CINT").show())
    
    //println(df.groupBy("FlightNum").sum("DayOfWeek").show())

    println("That is all folks.........")
    System.exit(0);

}