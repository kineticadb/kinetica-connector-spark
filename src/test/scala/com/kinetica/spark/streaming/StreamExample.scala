package com.kinetica.spark.streaming

import java.io.File
import java.io.Serializable
import java.util.ArrayList
import java.util.Arrays
import java.util.List
import java.util.concurrent.ThreadLocalRandom

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaIterator

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import com.gpudb._
import com.gpudb.avro.generic.GenericRecord
import com.kinetica.spark.LoaderParams
import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.apache.spark.sql.types._

import StreamExample.NEW_DATA_INTERVAL_SECS
import StreamExample.STREAM_POLL_INTERVAL_SECS

object StreamExample extends LazyLogging {

    def main(args: Array[String]): Unit = {
        System.setProperty("spark.sql.warehouse.dir", "file:///C:/1SPARK/spark-warehouse");
        System.setProperty("hadoop.home.dir", "c:/1SPARK/")
        if (args.length < 4) {
            throw new Exception("<host ip> <srcTableName> <destTableName> <insertBatchSize> [<username> <password>]")
        }
        val example: StreamExample = new StreamExample(args)
        try example.runTest()
        catch {
            case e: Exception => {
                logger.error("Problem streaming data between GPUdb and Spark", e)
                System.exit(-2)
            }

        }
    }

    private val NEW_DATA_INTERVAL_SECS: Int = 10
    private val STREAM_POLL_INTERVAL_SECS: Int = NEW_DATA_INTERVAL_SECS
    
    var schema : StructType = null
    
    def getStruct(lp:LoaderParams) : StructType = {
        if ( schema == null ) {
           schema = new StructType()
           val myType = Type.fromTable(lp.getGpudb(), lp.tablename)
           for( column <- myType.getColumns ) {
               logger.debug(" ############ name in struct column is {}/{}/{}", column.getName, column.getType().toString(), column.getProperties())
               if( column.getType().toString().contains("java.lang.Double") )
                   schema = schema.add(new StructField(column.getName, DataTypes.DoubleType, column.isNullable()))
               else if (column.getType().toString().contains("java.lang.Float")) 
                   schema = schema.add(new StructField(column.getName, DataTypes.FloatType, column.isNullable()))                     
               else if (column.getType().toString().contains("java.lang.Integer")) 
                   schema = schema.add(new StructField(column.getName, DataTypes.IntegerType, column.isNullable()))
               else if (column.getType().toString().contains("java.lang.Long")) 
                   schema = schema.add(new StructField(column.getName, DataTypes.LongType, column.isNullable()))
               else if (column.getType().toString().contains("java.lang.String")) {
                   schema = schema.add(new StructField(column.getName, DataTypes.StringType, column.isNullable()))
               }
           } 
        }
        schema
    }
    
    def frow(t: AvroWrapper): Row = {
        
        //println(" ############ Executing frow ########## ")
        
        val inRecord: GenericRecord = t.getGenericRecord
        val myType = new Type(t.getSchemaString())
        var out = new ArrayList[Any](myType.getColumns.size)
        var ii = 0
        for( column <- myType.getColumns ) {
            if( column.getType.getCanonicalName.contains("java.lang.Double") )
                out.add(inRecord.get(column.getName).asInstanceOf[Double])
            else if( column.getType.getCanonicalName.contains("java.lang.Float") )
                out.add(inRecord.get(column.getName).asInstanceOf[Float])
            else if( column.getType.getCanonicalName.contains("java.lang.Integer") )
                out.add(inRecord.get(column.getName).asInstanceOf[Integer])
            else if( column.getType.getCanonicalName.contains("java.lang.Long") )
                out.add(inRecord.get(column.getName).asInstanceOf[Long])
            else if( column.getType.getCanonicalName.contains("java.lang.String") )
                out.add(inRecord.get(column.getName).asInstanceOf[String])
            ii = ii + 1;
            //println(" ############ Executing frow 22 ########## ")
        }
        val row = Row.fromSeq(out)
        row
    }
}

/**
 * Performs a test of streaming through Spark from GPUdb to GPUdb.
 *
 * The example uses Spark to receive a data feed from a GPUdb table monitor as
 * the streaming data source.  A GPUdb table monitor provides a stream of new
 * data inserted into the source table as a ZMQ queue.  Spark will subscribe to
 * the queue, receive data, map the incoming data into the format of the target
 * GPUdb table, and write mapped records to the target GPUdb via Spark stream.
 *
 * @author dkatz
 */
@SerialVersionUID(3245471188522114052L)
class StreamExample(args: Array[String]) extends Serializable with LazyLogging {

    private var sparkAppName: String = getClass.getSimpleName

    logger.debug(" ********** SparkKinetica Stream Example class main constructor ********** ")
    
    // Do more robust args checking if so desired.
    val host = args(0)
    val srcTableName = args(1)
    val destTableName = args(2)
    val ingestBatchSize = args(3)
    val username = if (args.length > 4) args(4) else ""
    val password = if (args.length > 5) args(5) else ""
    
    /**
     * Launches a background process that will supply the streaming data source
     * table with new records on the defined interval.  New records added by
     * this method will be queued by the table monitor monitoring the source
     * table and be made available to the Spark stream receiving monitored data.
     */
    private def launchAdder(lp : LoaderParams): Unit = {
        new Thread() {
            override def run(): Unit = {
                val writer: GPUdbWriter[Record] = new GPUdbWriter[Record](lp)
                val myType = Type.fromTable(lp.getGpudb(), lp.tablename);
                while (true) {
                    Thread.sleep(NEW_DATA_INTERVAL_SECS * 1000)
                    // Add records to source table which we will monitor
                    // Creating same number of records as batch size for no good reason...
                    for (a <- 1 to lp.insertSize) {
                        writer.write(GPUdbUtil.getData(myType))
                    }
                }
            }
        }.start()
    }

    /**
     * Establishes a Spark stream from a GPUdb table monitor and writes streamed
     * records to GPUdb
     *
     * @throws GPUdbException if an error occurs accessing GPUdb
     */
    private def runTest(): Unit = {

        val URL = s"http://${host}:9191"
        val STREAM_URL = s"tcp://${host}:9002"
        val streamingOptions = Map(
            "database.url" -> URL,
            "database.stream_url" -> STREAM_URL,
            "database.username" -> username,
            "database.password" -> password,
            "ingester.batch_size" -> ingestBatchSize,
            "ingester.num_threads" -> "4",
            "table.name" -> srcTableName
        )
        
        val sc = new SparkConf().setAppName(sparkAppName)

        val lp = new LoaderParams(SparkSessionSingleton.getInstance(sc).sparkContext, streamingOptions)
        // table monitor to queue to the data stream
        launchAdder(lp)

        val ssc = new StreamingContext(sc, Durations.seconds(STREAM_POLL_INTERVAL_SECS))

        val receiver: GPUdbReceiver = new GPUdbReceiver(lp)
        val inStream: ReceiverInputDStream[AvroWrapper] = ssc.receiverStream(receiver)
       
        val outStream: DStream[Row] = inStream.map(StreamExample.frow)
        val schema = StreamExample.getStruct(lp)
        logger.debug(" Schema for dataframe based on kinetica type is = " + schema.simpleString)
        
        logger.info(" Destination table is = " + destTableName)
        val ingestOptions = Map(
            "database.url" -> URL,
            "database.jdbc_url" -> s"jdbc:simba://${host}:9292;URL=${URL}",
            "database.username" -> username,
            "database.password" -> password,
            "ingester.batch_size" -> ingestBatchSize,
            "ingester.num_threads" -> "4",
            "table.name" -> destTableName,
            "table.map_columns_by_name" -> "true",
            "table.create" -> "true"
        )          
          
        outStream.foreachRDD { 
            (rdd: RDD[Row], time: Time) =>
            // Get the singleton instance of SparkSession
            val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
            import spark.implicits._

            if(!rdd.isEmpty()) {
                rdd.repartition(10);
                val df = spark.createDataFrame(rdd, schema)
                //df.printSchema()
                df.write.format("com.kinetica.spark").options(ingestOptions).save()
                df.write.format("csv").mode(SaveMode.Append).save("StreamExample.out")
            }
        }

        ssc.start()
        ssc.awaitTermination()
    }

    private def parseArgs(args: Array[String]): PropertiesConfiguration = {
        val argList: List[String] = new ArrayList[String](Arrays.asList(args: _*))
        val propPath: String = argList.remove(0)
        val propFile: File = new File(propPath)
        logger.info("Reading properties from file: {}", propFile)

        val conf = new PropertiesConfiguration(propPath);
        conf
    }
}

import org.apache.spark.sql.SparkSession
object SparkSessionSingleton {

    @transient private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
        if (instance == null) {
            instance = SparkSession
                .builder
                .config(sparkConf)
                .getOrCreate()
        }
        instance
    }
}
