package com.kinetica.spark.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StructType

import com.typesafe.scalalogging.LazyLogging

object Flatten extends LazyLogging {

    def main(args: Array[String]): Unit = {
        System.setProperty("spark.sql.warehouse.dir", "file:///C:/1SPARK/spark-warehouse");
        System.setProperty("hadoop.home.dir", "c:/1CONNECTOR/SPARK/")

        val spark = SparkSession.builder()
            .appName("Flatten")
            .enableHiveSupport()
            .master("local")
            .getOrCreate
            
        //val df = spark.read.option("multiLine", "true").json("c:/1CONNECTOR/SPARK/data/nested.json")

        /*
        val sc = spark.sparkContext
        val rdd = sc.parallelize(Seq((1, Seq(2,3,4), Seq(5,6,7)),
                    (2, Seq(3,4,5), Seq(6,7,8)), (3, Seq(4,5,6), Seq(7,8,9))) , 2)
        val df = spark.createDataFrame(rdd).toDF("a", "b", "c")
		*/

        //val df = anotherJsonDF(spark)
            
        val df = yetAnotherJsonDF(spark)  
        
        //val df = whyNotOneMore(spark) 
            
        df.printSchema()
        df.show()

        val newDf = flatten_all(df)

        newDf.printSchema()
        newDf.show(100, false)

        spark.close()
        spark.stop()
    }

    def flatten_all(df: DataFrame): DataFrame = {
        val frame = flatten_frame(df)
        val (frame2, have_array) = flatten_array(frame)

        //System.out.println(" ################################## ")
        //frame2.printSchema()
        //System.out.println(" ################################## " + have_array)

        if (have_array)
            return flatten_all(frame2)
        else
            return frame2
    }

    import scala.collection.mutable.ListBuffer
    private def flatten_array(frame: DataFrame): (DataFrame, Boolean) = {
        var have_array = false
        var aliased_columns = new ListBuffer[Column]()

        val fields = frame.schema.fields

        fields.flatMap(f => {
            f.dataType match {
                case st: ArrayType => {
                    if (have_array == false) {
                        have_array = true
                        //System.out.println(" I am array " + f.name + "/" + f.dataType)
                        //System.out.println(" Exploding array " + explode(frame.col(f.name)))
                        aliased_columns += explode(frame.col(f.name)).alias(f.name)
                    } else {
                        //System.out.println(" I am array but next time" + f.name + "/" + f.dataType)
                        aliased_columns += frame.col(f.name)
                    }

                }
                case _ => {
                    //System.out.println(" I am NOT array " + f.name + "/" + f.dataType)
                    aliased_columns += frame.col(f.name)
                }
            }
        })
        //System.out.println(" And in the end.... " + aliased_columns.length)
        //frame.select(aliased_columns: _*).show()
        return (frame.select(aliased_columns: _*), have_array)
    }

    private def flatten_frame(dataFrame: DataFrame, prefix: String = null): DataFrame = {
        val flattenedSchema = flattenStruct(dataFrame.schema)
        val renamedCols = flattenedSchema.map(name => col(name.toString()).as(name.toString().replace(".", "_")))
        var newDf = dataFrame.select(renamedCols: _*)

        //System.out.println("------------------------------------")
        //newDf.printSchema()
        //System.out.println("------------------------------------")
        return newDf
    }

    private def flattenStruct(schema: StructType, prefix: String = null): Array[Column] = {
        schema.fields.flatMap(f => {
            val colName = if (prefix == null) f.name else (prefix + "." + f.name)
            f.dataType match {
                case st: StructType => flattenStruct(st, colName)
                case _ => Array(col(colName))
            }
        })
    }
    
    private def whyNotOneMore(spark: SparkSession): DataFrame = {
        import spark.implicits._ 
        /*
        val sequ = Seq("""
        {
          "a": {
             "b": 1
           }
        }""").toDS
        * 
        */
        val sequ = Seq("""
        {
          "id": 0,
          "a": {
            "b": {"d": 1, "e": 2},
            "c": {"d": 3, "e": 4}
          }
        }""").toDS
        val df = spark.read.json(sequ)
        return df
    }

    private def anotherJsonDF(spark: SparkSession): DataFrame = {
        import spark.implicits._  
        val sequ = Seq("""
        {
          "hierarchy": {
            "record": {
              "id": 1,
              "record": [
                {
                  "id": 6,
                  "record": [
                    {
                      "id": 7
                    }
                  ]
                }
              ]
            }
          },
          "type": "record"
        }""").toDS
        val df = spark.read.json(sequ)
        return df
    }

    
    private def yetAnotherJsonDF(spark: SparkSession): DataFrame = {
        import spark.implicits._  
        import org.apache.spark.sql.types._
        val schema = new StructType()
          .add("dc_id", StringType)                               // data center where data was posted to Kafka cluster
          .add("source",                                          // info about the source of alarm
            MapType(                                              // define this as a Map(Key->value)
              StringType,
              new StructType()
              .add("description", StringType)
              .add("ip", StringType)
              .add("id", LongType)
              .add("temp", LongType)
              .add("c02_level", LongType)
              .add("geo", 
                 new StructType()
                  .add("lat", DoubleType)
                  .add("long", DoubleType)
                )
              )
            )
        val seq = Seq("""
        [{
       "dc_id": "dc-101",
       "source": {
           "sensor-igauge": {
             "id": 10,
             "ip": "68.28.91.22",
             "description": "Sensor attached to the container ceilings",
             "temp":35,
             "c02_level": 1475,
             "geo": {"lat":38.00, "long":97.00}                        
           },
           "sensor-ipad": {
             "id": 13,
             "ip": "67.185.72.1",
             "description": "Sensor ipad attached to carbon cylinders",
             "temp": 34,
             "c02_level": 1370,
             "geo": {"lat":47.41, "long":-122.00}
           },
           "sensor-inest": {
             "id": 8,
             "ip": "208.109.163.218",
             "description": "Sensor attached to the factory ceilings",
             "temp": 40,
             "c02_level": 1346,
             "geo": {"lat":33.61, "long":-111.89}
           },
           "sensor-istick": {
             "id": 5,
             "ip": "204.116.105.67",
             "description": "Sensor embedded in exhaust pipes in the ceilings",
             "temp": 40,
             "c02_level": 1574,
             "geo": {"lat":35.93, "long":-85.46}
           }
         }
       }, {
       "dc_id": "dc-102",
       "source": {
           "sensor-igauge": {
             "id": 100,
             "ip": "68.28.91.22",
             "description": "Sensor attached to the container ceilings",
             "temp":35,
             "c02_level": 1475,
             "geo": {"lat":38.00, "long":97.00}                        
           },
           "sensor-ipad": {
             "id": 130,
             "ip": "67.185.72.1",
             "description": "Sensor ipad attached to carbon cylinders",
             "temp": 34,
             "c02_level": 1370,
             "geo": {"lat":47.41, "long":-122.00}
           },
           "sensor-inest": {
             "id": 8,
             "ip": "208.109.163.218",
             "description": "Sensor attached to the factory ceilings",
             "temp": 40,
             "c02_level": 1346,
             "geo": {"lat":33.61, "long":-111.89}
           },
           "sensor-istick": {
             "id": 5,
             "ip": "204.116.105.67",
             "description": "Sensor embedded in exhaust pipes in the ceilings",
             "temp": 40,
             "c02_level": 1574,
             "geo": {"lat":35.93, "long":-85.46}
           }
         }
       }, {
       "dc_id": "dc-103",
       "source": {
           "sensor-alpha": {
             "id": 100,
             "ip": "68.28.91.22",
             "description": "Sensor attached to the container ceilings",
             "temp":35,
             "c02_level": 1475,
             "geo": {"lat":38.00, "long":97.00}                        
           },
           "sensor-ipad": {
             "id": 130,
             "ip": "67.185.72.1",
             "description": "Sensor ipad attached to carbon cylinders",
             "temp": 34,
             "c02_level": 1370,
             "geo": {"lat":47.41, "long":-122.00}
           },
           "sensor-gamma": {
             "id": 8,
             "ip": "208.109.163.218",
             "description": "Sensor attached to the factory ceilings",
             "temp": 40,
             "c02_level": 1346,
             "geo": {"lat":33.61, "long":-111.89}
           },
           "sensor-istick": {
             "id": 5,
             "ip": "204.116.105.67",
             "description": "Sensor embedded in exhaust pipes in the ceilings",
             "temp": 40,
             "c02_level": 1574,
             "geo": {"lat":35.93, "long":-85.46}
           }
         }
        }] """).toDS
    //val df = spark.read.json(seq)
    
    val df = spark.read.schema(schema).json(seq)
    val x = df.toJSON
    val xf = spark.read.json(x)
    return xf
    }     
}