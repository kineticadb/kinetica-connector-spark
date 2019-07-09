package com.kinetica.spark.util.json

import java.io.BufferedOutputStream

import com.gpudb.GPUdbBase
import com.gpudb.protocol.{CreateTableRequest, CreateTableResponse, CreateTypeRequest, ShowTableRequest}
import com.kinetica.spark.LoaderParams
import com.kinetica.spark.loader.LoaderConfiguration
import com.kinetica.spark.util.ConfigurationConstants._
import com.kinetica.spark.util.json.KineticaJsonSchema.{KineticaSchemaMetadata, KineticaSchemaRoot}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.util.Try

class KineticaJsonSchemaHelper(val loaderConfig: LoaderConfiguration) extends LazyLogging {

  logger.debug("*********************** Constructor: SchemaHandler")

  def this(parameters: scala.collection.immutable.Map[String, String], @transient sparkSession: SparkSession)
  {
    this(new LoaderConfiguration(sparkSession.sparkContext, parameters))
  }

  require(loaderConfig.kineticaURL != null, s"Parameter $KINETICA_URL_PARAM cannot be null")

  private var jsonDataFileName = loaderConfig.jsonSchemaFilename


  /**
    * reading a path requires that we either have an absolute path to the json file, or can infer one from the provided path
    */
  def getValidPath(stringPath: String): Path = {
    stringPath match {
      case x if x == null || x.trim.isEmpty => getValidPath(None)
      case s => getValidPath(Some(new Path(s)))
    }
  }

  /**
    * reading a path requires that we either have an absolute path to the json file, or can infer one from the provided path
    */
  def getValidPath(path: Option[Path]): Path = {
    path match {

      // if path ends with .json use it
      case Some(p) if p.getName.endsWith(".json") => p

      // otherwise, if json file name is absolute use that
      case _ if new Path(jsonDataFileName).isUriPathAbsolute =>
        new Path(jsonDataFileName)

      // create a new path with the json filename appended
      case Some(p) => new Path(p,jsonDataFileName)

      // we've exhausted all options
      case _ => {
        throw new IllegalArgumentException("use of Kinetica Json Template requires either a data path or an absolute json filename")
      }
    }
  }

  def getKineticaTableSchema(): KineticaSchemaMetadata = {

    val gpudb = loaderConfig.getGpudb()

    val st = new ShowTableRequest()
    st.setTableName(loaderConfig.tablename)
    st.setOptions(mutable.Map(ShowTableRequest.Options.GET_COLUMN_INFO -> ShowTableRequest.Options.TRUE))
    val table = gpudb.showTable(st)
    val isReplicated = Try(table.getTableDescriptions.asScala.head.contains("REPLICATED")).getOrElse(false)
    val typeId = table.getTypeIds.asScala.head
    val typeInfo = gpudb.showTypes(typeId,null,null)
    KineticaJsonSchema.toKineticaSchemaMetadata(typeInfo, isReplicated)
  }

  private def getFileSystem(path: Path): FileSystem = {

    val hadoopConfig = Try(loaderConfig.sparkContext.get.hadoopConfiguration).getOrElse(new Configuration())

    if(path.isUriPathAbsolute)
    {
      FileSystem.get(path.toUri, hadoopConfig)
    } else {
      FileSystem.get(hadoopConfig)
    }
  }


  def saveKineticaSchema(path: String, kineticaSchemaMetadata: KineticaSchemaMetadata): Unit = saveKineticaSchema(getValidPath(path), kineticaSchemaMetadata)

  def saveKineticaSchema(path: Path, kineticaSchemaMetadata: KineticaSchemaMetadata): Unit = {

    val schemaJson = KineticaJsonSchema.encodeSchemaToJson(KineticaSchemaRoot(Some(kineticaSchemaMetadata)))

    val targetPath = getValidPath(Some(path))

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

  def loadKineticaSchema(): Option[KineticaSchemaMetadata] = loadKineticaSchema(getValidPath(loaderConfig.dataPath))

  def loadKineticaSchema(path: String): Option[KineticaSchemaMetadata] = loadKineticaSchema(new Path(path))

  def loadKineticaSchema(path: Path): Option[KineticaSchemaMetadata] = {

    val targetPath = getValidPath(Some(path))

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


  def createTableFromSchema(): CreateTableResponse = {
    loadKineticaSchema() match {
      case Some(s) =>  createTableFromSchema(s)
      case _ => {
        throw new IllegalArgumentException(s"Cannot create kinetica table ${loaderConfig.tablename} because a schema could not be properly loaded from the json file")
      }
    }
  }

  def createTableFromSchema(kineticaSchemaMetadata: KineticaSchemaMetadata): CreateTableResponse = createTableFromSchema(loaderConfig.tablename, loaderConfig.schemaname, kineticaSchemaMetadata)

  def createTableFromSchema(tableName: String, schemaName: String, kineticaSchemaMetadata: KineticaSchemaMetadata) = {

    val gpudb = loaderConfig.getGpudb()

    logger.info( "Creating new type for table <{}.{}>", schemaName, tableName)

    val newTypeId = gpudb.createType(getCreateTypeRequest(kineticaSchemaMetadata)).getTypeId

    // does table already exist? drop if so
    if(gpudb.hasTable(tableName, null).getTableExists)
    {
      gpudb.clearTable(tableName, gpudb.getUsername, null)
    }

    val createTable = new CreateTableRequest()
    createTable.setTableName(tableName)
    createTable.setTypeId(newTypeId)
    createTable.setOptions(GPUdbBase.options(CreateTableRequest.Options.COLLECTION_NAME, schemaName))

    if(kineticaSchemaMetadata.isReplicated)
    {
      createTable.setOptions(
        GPUdbBase.options(
          CreateTableRequest.Options.COLLECTION_NAME, schemaName,
          CreateTableRequest.Options.IS_REPLICATED, CreateTableRequest.Options.TRUE))

      // force replicated table to not use multihead
      loaderConfig.setMultiHead(false)
      loaderConfig.setTableReplicated(true)
    }

    logger.info( "Creating table <{}.{}> for type()", schemaName, tableName, newTypeId)
    gpudb.createTable(createTable)
  }




}
