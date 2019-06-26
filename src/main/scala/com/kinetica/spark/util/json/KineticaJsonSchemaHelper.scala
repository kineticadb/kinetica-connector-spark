package com.kinetica.spark.util.json

import java.io.BufferedOutputStream

import com.gpudb.GPUdbBase
import com.gpudb.protocol.{CreateTableRequest, CreateTableResponse, CreateTypeRequest, ShowTableRequest}
import com.kinetica.spark.LoaderParams
import com.kinetica.spark.util.ConfigurationConstants._
import com.kinetica.spark.util.json.KineticaJsonSchema.{KineticaSchemaMetadata, KineticaSchemaRoot}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.Map
import scala.util.Try

class KineticaJsonSchemaHelper(val lp: LoaderParams) extends LazyLogging {

  logger.debug("*********************** Constructor: SchemaHandler")

  def this(parameters: scala.collection.immutable.Map[String, String], @transient sparkSession: SparkSession)
  {
    this(new LoaderParams(Some(sparkSession.sparkContext), parameters))
  }

  require(lp.kineticaURL != null, s"Parameter $KINETICA_URL_PARAM cannot be null")

  val jsonDataFileName = lp.jsonSchemaFilename

  def getKineticaTableSchema(): KineticaSchemaMetadata = {

    val gpudb = lp.getGpudb()

    val st = new ShowTableRequest()
    st.setTableName(lp.tablename)
    st.setOptions(Map(ShowTableRequest.Options.GET_COLUMN_INFO -> ShowTableRequest.Options.TRUE))
    val table = gpudb.showTable(st)
    val isReplicated = Try(table.getTableDescriptions.asScala(0).contains("REPLICATED")).getOrElse(false)
    val typeId = table.getTypeIds.asScala(0)
    val typeInfo = gpudb.showTypes(typeId,null,null)
    KineticaJsonSchema.toKineticaSchemaMetadata(typeInfo, isReplicated)
  }

  private def getFileSystem(path: Path): FileSystem = {

    val hadoopConfig = Try(lp.sparkContext.get.hadoopConfiguration).getOrElse(new Configuration())

    if(path.isUriPathAbsolute)
    {
      FileSystem.get(path.toUri, hadoopConfig)
    } else {
      FileSystem.get(hadoopConfig)
    }
  }


  def saveKineticaSchema(path: String, kineticaSchemaMetadata: KineticaSchemaMetadata): Unit = saveKineticaSchema(new Path(path), kineticaSchemaMetadata)

  def saveKineticaSchema(path: Path, kineticaSchemaMetadata: KineticaSchemaMetadata): Unit = {

    val schemaJson = KineticaJsonSchema.encodeSchemaToJson(KineticaSchemaRoot(Some(kineticaSchemaMetadata)))

    val targetPath = if(path.getName.endsWith(".json")) path else new Path(path,jsonDataFileName)

    val hdfs = getFileSystem(targetPath)

    hdfs.delete(new Path(s"$targetPath"), true)

    if(!hdfs.exists(targetPath.getParent))
    {
      hdfs.mkdirs(targetPath.getParent)
    }

    val output = hdfs.create(targetPath, true)
    val os = new BufferedOutputStream(output)
    os.write(schemaJson.getBytes)
    os.close()

  }

  def loadKineticaSchema(path: String): Option[KineticaSchemaMetadata] = loadKineticaSchema(new Path(path))

  def loadKineticaSchema(path: Path): Option[KineticaSchemaMetadata] = {

    val targetPath = if(path.getName.endsWith(".json")) path else new Path(path,jsonDataFileName)

    val hdfs = getFileSystem(targetPath)

    if (hdfs.exists(targetPath) && hdfs.getFileStatus(targetPath).getLen > 0) {

      val stream = hdfs.open(targetPath)
      def readLines = Stream.cons(stream.readLine, Stream.continually( stream.readLine))
      val contents = readLines.takeWhile(_ != null).toList.mkString

      KineticaJsonSchema.decodeSchemaFromJson(contents).kinetica_schema

    } else {
      throw new Exception(s"Kinetica schema file at $targetPath could not be found")
    }
  }

  private def getCreateTypeRequest(kineticaSchemaMetadata: KineticaSchemaMetadata): CreateTypeRequest = {
    val newType = new CreateTypeRequest()
    newType.setProperties(kineticaSchemaMetadata.typeProperties.map{case (k, v) => k -> v.toList.asJava}.asJava)
    newType.setTypeDefinition(KineticaJsonSchema.encodeKineticaSchemaDefinition(kineticaSchemaMetadata.typeSchema))
    newType
  }

  def createTableFromSchema(kineticaSchemaMetadata: KineticaSchemaMetadata): CreateTableResponse = createTableFromSchema(lp.tablename, kineticaSchemaMetadata)


  def createTableFromSchema(tableName: String, kineticaSchemaMetadata: KineticaSchemaMetadata) = {

    val gpudb = lp.getGpudb()
    val tableParams: Array[String] = tableName.split("\\.")

    var schemaName: String = if (tableParams.length == 2) { tableParams(0).stripPrefix("[").stripSuffix("]") } else lp.schemaname
    val table: String = if (tableParams.length == 2) { tableParams(1).stripPrefix("[").stripSuffix("]") } else tableName

    logger.info( "Creating new type for table <{}.{}>", schemaName, table)

    val newTypeId = gpudb.createType(getCreateTypeRequest(kineticaSchemaMetadata)).getTypeId

    // does table already exist? drop if so
    if(gpudb.hasTable(table, null).getTableExists)
    {
      gpudb.clearTable(table, gpudb.getUsername, null)
    }

    val createTable = new CreateTableRequest()
    createTable.setTableName(table)
    createTable.setTypeId(newTypeId)
    createTable.setOptions(GPUdbBase.options(CreateTableRequest.Options.COLLECTION_NAME, schemaName))

    if(kineticaSchemaMetadata.isReplicated)
    {
      createTable.setOptions(
        GPUdbBase.options(
          CreateTableRequest.Options.COLLECTION_NAME, schemaName,
          CreateTableRequest.Options.IS_REPLICATED, CreateTableRequest.Options.TRUE))

      // force replicated table to not use multihead
      lp.setMultiHead(false)
    }

    logger.info( "Creating table <{}.{}> for type()", schemaName, table, newTypeId)
    gpudb.createTable(createTable)
  }




}
