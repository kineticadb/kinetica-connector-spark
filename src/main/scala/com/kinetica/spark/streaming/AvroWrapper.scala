package com.kinetica.spark.streaming

import com.gpudb.Avro
import com.gpudb.GPUdbException
import com.gpudb.avro.Schema
import com.gpudb.avro.generic.GenericRecord
import java.io.Serializable
import java.nio.ByteBuffer
import scala.collection.JavaConversions._

@SerialVersionUID(5845927120287011691L)
class AvroWrapper(private var schemaStr: String, private var bytes: Array[Byte])
    extends Serializable {

  def getGenericRecord(): GenericRecord =
    Avro.decode(new Schema.Parser().setValidate(true).parse(schemaStr),
                ByteBuffer.wrap(bytes))

  override def toString(): String = {
    var retVal: String = null
    retVal = getGenericRecord.toString
    retVal
  }

}
