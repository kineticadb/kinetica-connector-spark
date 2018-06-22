package com.kinetica.spark.streaming

import java.net.InetAddress
import java.net.NetworkInterface
import java.net.SocketException
import java.util.Enumeration
import com.gpudb._
import com.gpudb.protocol.CreateTableRequest
import org.apache.hadoop.conf.Configuration

//remove if not needed
import scala.collection.JavaConversions._

object GPUdbUtil {
    def getData(myType:Type) : Record = {
        val gr = myType.newInstance();
        val columnCount = myType.getColumnCount
        val r = scala.util.Random
        for (column <- myType.getColumns) { 
            
            //println(" &&&&&&& " + column.getProperties.mkString)
            if (column.getType().toString().contains("java.lang.Double")) {
                gr.put(column.getName(), r.nextDouble())
            } else if (column.getType().toString().contains("java.lang.Float")) {
                gr.put(column.getName(), r.nextFloat())
            } else if (column.getType().toString().contains("java.lang.Integer")) {
                if ( column.getProperties.mkString.toLowerCase.contains("int16") ) {
                    gr.put(column.getName(), r.nextInt(2^16))
                } else if ( column.getProperties.mkString.toLowerCase.contains("int8") ) { 
                    gr.put(column.getName(), r.nextInt(2^8))
                } else gr.put(column.getName(), r.nextInt())
            } else if (column.getType().toString().contains("java.lang.Long")) {
                if ( column.getProperties.mkString.toLowerCase.contains("timestamp") ) {
                    var ts = r.nextLong()
                    ts = if (ts < -30610224000000L) -30610224000000L else ts
                    ts = if (ts > 29379542399999L) 29379542399999L else ts
                    gr.put(column.getName(), ts);
                } else {
                    gr.put(column.getName(), r.nextLong())
                }
            } else {
                if ( column.getProperties.mkString.toLowerCase.contains("date") )
                    gr.put(column.getName(), "1963-12-25");
                else if ( column.getProperties.mkString.toLowerCase.contains("ipv4") )
                    gr.put(column.getName(), "172.12.24.63");
                else if ( column.getProperties.mkString.toLowerCase.contains("decimal") )
                    gr.put(column.getName(), "155.52");
                else gr.put(column.getName(), scala.util.Random.alphanumeric.take(1).mkString);
            }
        }
        gr
    }

}
