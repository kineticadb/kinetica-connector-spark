package com.kinetica.spark.loader

import java.io.Serializable

import scala.beans.{BeanProperty, BooleanBeanProperty}
import com.kinetica.spark.util.ConfigurationConstants._
import com.typesafe.scalalogging.LazyLogging
import com.kinetica.spark.LoaderParams
import org.apache.spark.SparkContext

@SerialVersionUID(-2502861044221136156L)
class LoaderConfiguration(sc:SparkContext, params: Map[String, String])
    extends LoaderParams(Option.apply(sc), params) with Serializable with LazyLogging {

    @BeanProperty
    val sqlFileName: String = params.getOrElse(CONNECTOR_SQLFILE_PARAM, null)

    @BeanProperty
    val dataPath: String = params.getOrElse(CONNECTOR_DATAPATH_PARAM, null)

    @BeanProperty
    val dataFormat: String = params.getOrElse(CONNECTOR_DATAFORMAT_PARAM, null)

    @BeanProperty
    val useTemplates: Boolean = params.getOrElse(KINETICA_USETEMPLATES_PARAM, "false").toBoolean

    @BeanProperty
    val partitionRows: Int = params.getOrElse(CONNECTOR_ROWSPERPARTITION_PARAM, "-1").toInt

    @BeanProperty
    val csvHeader: Boolean = params.getOrElse(KINETICA_CSV_HEADER, "false").toBoolean

    // add support for json-based templates
    @BooleanBeanProperty
    val useJsonTemplate = params.getOrElse(KINETICA_USE_JSON_SCHEMA, "false").toBoolean

    @BeanProperty
    val jsonSchemaFilename = params.getOrElse(CONNECTOR_JSON_SCHEMA_FILENAME_PARAM, KINETICA_DEFAULT_JSON_FILE)

    // Use the datasource v1 API path by default
    @BeanProperty
    val datasourceVersion: String = params.getOrElse(SPARK_DATASOURCE_VERSION, "v1")


}
