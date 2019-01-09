package com.kinetica.spark.util.table


import org.apache.spark.api.java.function.MapPartitionsFunction
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import java.util.ArrayList
import java.util.Iterator
import java.util.List

import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import org.apache.spark.sql.SQLImplicits

object TypeStringProcessor {

  def getMaxStringLen(ds: DataFrame, columnName: String): DataFrame = {
    val integerEncoder: Encoder[Integer] = Encoders.INT
    //println("========== getMaxStringLen on " + columnName)
    ds.createOrReplaceTempView("temptable")
    val sc = ds.sparkSession.sqlContext
    val colNameWithoutQuotes = columnName.substring(1, columnName.length()-1)
    val sqlString = s"select max(length(${colNameWithoutQuotes})) from temptable"
    //println(" Sql string for temp table is " + sqlString)
    val df = sc.sql(sqlString)
    df
  }  
}
