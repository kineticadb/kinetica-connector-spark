package com.kinetica.spark.streaming

import java.io.File
import java.io.Serializable
import java.util.ArrayList
import java.util.Arrays
import java.util.List
import java.util.concurrent.ThreadLocalRandom

//remove if not needed
import scala.collection.JavaConversions.asScalaBuffer
//remove if not needed
import scala.collection.JavaConversions.asScalaIterator

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import com.gpudb.avro.generic.GenericRecord
import com.kinetica.spark.LoaderParams
import com.typesafe.scalalogging.LazyLogging

import com.kinetica.spark.PersonRecord

import org.apache.spark.sql.Row

object StreamExample extends LazyLogging {

    def main(args: Array[String]): Unit = {
        System.setProperty("spark.sql.warehouse.dir", "file:///C:/1SPARK/spark-warehouse");
        System.setProperty("hadoop.home.dir", "c:/1SPARK/")
        if (args.length < 1) {
            throw new Exception("First argument must be a properties file.")
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

    private val peopleNames: Array[String] = Array("John", "Jeremy", "Andrew", "Kyle", "Jordan", "Meem", "Ying")

    private def getMorePeople(): List[PersonRecord] = {
        val people: List[PersonRecord] = new ArrayList[PersonRecord]()
        // Create test records
        for (personName <- peopleNames)
            people.add(
                new PersonRecord(
                    ThreadLocalRandom.current().nextInt(0, java.lang.Integer.MAX_VALUE),
                    personName,
                    System.currentTimeMillis()))
        people
    }

    def fpr(t: AvroWrapper): PersonRecord = {
        var record: PersonRecord = null
        val inRecord: GenericRecord = t.getGenericRecord
        record = new PersonRecord(
            inRecord.get("id").asInstanceOf[Long],
            inRecord.get("name").asInstanceOf[String],
            inRecord.get("birthDate").asInstanceOf[Long])
        record
    }
    
    def frow(t: AvroWrapper): Row = {
        var row: Row = null
        val inRecord: GenericRecord = t.getGenericRecord
        row = Row(inRecord.get("id").asInstanceOf[Long], inRecord.get("name").asInstanceOf[String], inRecord.get("birthDate").asInstanceOf[Long])
        row
    }
}

import StreamExample.NEW_DATA_INTERVAL_SECS
import StreamExample.STREAM_POLL_INTERVAL_SECS
import StreamExample.getMorePeople
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
    private var gpudbCollectionName: String = "SparkExamples"
    //private var gpudbSourceTableName: String = sparkAppName + "_Source"
    private var gpudbTargetTableName: String = "StreamExample_Target"

    logger.debug(" ********** SparkKineticaDriver class main constructor ********** ")

    private val propertyConf: PropertiesConfiguration = parseArgs(args)

    var params = scala.collection.mutable.Map[String, String]()

    val propIt: Iterator[_] = propertyConf.getKeys()

    while (propIt.hasNext) {
        val key: String = propIt.next.toString
        val param: String = propertyConf.getString(key)
        logger.debug("config: {} = {}", key, param)
        params += (key -> param)
    }

    val immutableParams = params.map(kv => (kv._1, kv._2)).toMap
    val loaderConfig = new LoaderParams(immutableParams)

    /**
     * Loads Spark application configuration using the given file
     *
     * @param propFilePath path to Spark app configuration file
     * @throws IOException if properties file fails to load
     */
    private def loadProperties(propFilePath: String): Unit = {
    }

    /**
     * Launches a background process that will supply the streaming data source
     * table with new records on the defined interval.  New records added by
     * this method will be queued by the table monitor monitoring the source
     * table and be made available to the Spark stream receiving monitored data.
     */
    private def launchAdder(): Unit = {
        new Thread() {
            override def run(): Unit = {
                val writer: GPUdbWriter[PersonRecord] = new GPUdbWriter[PersonRecord](loaderConfig, loaderConfig.tablename)
                while (true) {
                    Thread.sleep(NEW_DATA_INTERVAL_SECS * 1000)
                    // Add records to process
                    for (person <- getMorePeople) writer.write(person)
                }
            }
        }.start()
    }

    import org.apache.spark.rdd.RDD
    import org.apache.spark.streaming.Time
    
    import org.apache.spark.sql.types._

    /**
     * Establishes a Spark stream from a GPUdb table monitor and writes streamed
     * records to GPUdb
     *
     * @throws GPUdbException if an error occurs accessing GPUdb
     */
    private def runTest(): Unit = {
        // Create source & destination tables in GPUdb for streaming processing
        GPUdbUtil.createTable(loaderConfig.kineticaURL, gpudbCollectionName, loaderConfig.tablename, classOf[PersonRecord])

        // table monitor to queue to the data stream
        launchAdder()

        //val sc: SparkContext = new SparkContext()
        val sc = new SparkConf().setAppName(sparkAppName).setMaster("local[*]")
        val ssc = new StreamingContext(sc, Durations.seconds(STREAM_POLL_INTERVAL_SECS))

        val receiver: GPUdbReceiver = new GPUdbReceiver(loaderConfig)
        val inStream: ReceiverInputDStream[AvroWrapper] = ssc.receiverStream(receiver)
       
        val outStream: DStream[Row] = inStream.map(StreamExample.frow)
        val schema = new StructType()
          .add(StructField("id", LongType, true))
          .add(StructField("val1", StringType, true))
          .add(StructField("val2", LongType, true))
          
        val host = "172.31.70.13"
        val URL = s"http://${host}:9191"
        val options = Map(
            "database.url" -> URL,
            "database.jdbc_url" -> s"jdbc:simba://${host}:9292;URL=${URL};ParentSet=MASTER",
            "database.username" -> "",
            "database.password" -> "",
            "ingester.ip_regex" -> "",
            "ingester.batch_size" -> "5",
            "ingester.num_threads" -> "4",
            "table.name" -> "gpudbTargetTableName",
            "table.is_replicated" -> "false",
            "table.update_on_existing_pk" -> "true",
            "table.map_columns_by_name" -> "false",
            "table.create" -> "true"
        )          
        import org.apache.spark.sql.SaveMode
        outStream.foreachRDD { 
            
            (rdd: RDD[Row], time: Time) =>
            // Get the singleton instance of SparkSession
            val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
            import spark.implicits._

            if(!rdd.isEmpty()) {
                val df = spark.createDataFrame(rdd, schema)
                df.write.format("com.kinetica.spark").options(options).save()
                df.write.format("csv").mode(SaveMode.Append).save("/home/shouvik/rough/STREAMDATAOUT")
            }
        }

        /*
		GPUdbUtil.createTable(loaderConfig.kineticaURL, gpudbCollectionName, gpudbTargetTableName, classOf[PersonRecord])
        val outStream: DStream[PersonRecord] = inStream.map(StreamExample.fpr)
        val writer: GPUdbWriter[PersonRecord] = new GPUdbWriter[PersonRecord](loaderConfig, gpudbTargetTableName)
        writer.write(outStream)
        inStream.print()
		*/
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

case class Record(person: PersonRecord)

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
