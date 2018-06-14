package com.kinetica.spark.streaming

import com.gpudb.GPUdb

import com.gpudb.GPUdbException

import com.gpudb.Type

import com.gpudb.protocol.CreateTableMonitorResponse

import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming.receiver.Receiver

import org.zeromq.ZFrame

import org.zeromq.ZMQ

import org.zeromq.ZMQ.Socket

import org.zeromq.ZMsg

import GPUdbReceiver._

import com.kinetica.spark.LoaderParams

import com.typesafe.scalalogging.LazyLogging

//remove if not needed
import scala.collection.JavaConversions._

object GPUdbReceiver {

}

@SerialVersionUID(-5558211843505774265L)
class GPUdbReceiver( loaderConfig: LoaderParams)
    extends Receiver[AvroWrapper](StorageLevel.MEMORY_AND_DISK_2) with LazyLogging {
    
    private val config = loaderConfig

    private var gpudb: GPUdb = _

    private var zmqURL: String = _

    private var topicID: String = _

    private var objectType: Type = _

    override def onStart(): Unit = {
        new Thread() {
            override def run(): Unit = {
                receive()
            }
        }.start()
    }

    override def onStop(): Unit = {
        try gpudb.clearTableMonitor(topicID, null)
        catch {
            case ex: GPUdbException =>
                logger.warn("Problem clearing GPUdb table monitor", ex)
        }
    }

    /**
     * Create a socket connection and receive data until receiver is stopped
     */
    private def receive(): Unit = {
        try {
            // Create table monitor
            zmqURL = config.streamURL
            gpudb = new GPUdb(config.kineticaURL)
            val response: CreateTableMonitorResponse =
                gpudb.createTableMonitor(config.tablename, null)
            topicID = response.getTopicId
            objectType = Type.fromTable(gpudb, config.tablename)
            // Attach to table monitor streaming data queue
            val zmqContext: ZMQ.Context = ZMQ.context(1)
            val subscriber: Socket = zmqContext.socket(ZMQ.SUB)
            subscriber.connect(zmqURL)
            subscriber.subscribe(topicID.getBytes)
            subscriber.setReceiveTimeOut(1000)
            while (!isStopped) {
                val message: ZMsg = ZMsg.recvMsg(subscriber)
                if (message != null) {
                    var skip: Boolean = true
                    for (frame <- message) {
                        // Skip first ZFrame
                        if (skip) {
                            skip = false
                        } else {
                            if ((frame != null) && (frame.getData != null)) {
                                val schemaStr: String = objectType.getSchema.toString
                                store(new AvroWrapper(schemaStr, frame.getData))
                            }
                        }
                    }
                }
            }
            restart("Trying to connect again")
        } catch {
            case ce: Exception => restart("Could not connect", ce)

        }
    }
}
