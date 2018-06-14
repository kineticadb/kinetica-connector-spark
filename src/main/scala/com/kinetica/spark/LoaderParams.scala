package com.kinetica.spark

import com.gpudb.Type
import java.io.Serializable
import scala.beans.{ BeanProperty, BooleanBeanProperty }
import com.typesafe.scalalogging.LazyLogging
import com.kinetica.spark.util.ConfigurationConstants._

class LoaderParams extends Serializable with LazyLogging {
    
    @BeanProperty
    var timeoutMs: Int = 10000
    
    @BeanProperty
    var kineticaURL: String = null

    @BeanProperty
    var streamURL: String = null

    @BeanProperty
    var tableType: Type = null

    @BeanProperty
    var tablename: String = null

    @BeanProperty
    var schemaname: String = ""

    @BooleanBeanProperty
    var tableReplicated: Boolean = false

    @BeanProperty
    var KdbIpRegex: String = null

    @BeanProperty
    var insertSize: Int = 10000

    @BooleanBeanProperty
    var updateOnExistingPk: Boolean = false

    @BeanProperty
    var kusername: String = null

    @BeanProperty
    var kpassword: String = null

    @BeanProperty
    var threads: Int = 4

    @BeanProperty
    var jdbcURL: String = null

    @BooleanBeanProperty
    var createTable: Boolean = false

    @BooleanBeanProperty
    var mapToSchema: Boolean = true

    @BooleanBeanProperty
    var useSnappy: Boolean = true

    @BeanProperty
    var retryCount: Int = 2

    @BooleanBeanProperty
    var alterTable: Boolean = false
    
    @BeanProperty
    var multiHead: Boolean = false
    
    @BeanProperty
    var truncateTable: Boolean = false
    
    @BeanProperty
    var loaderPath: Boolean = false

    def this(params: Map[String, String]) = {
        this()
        require(params != null, "Config cannot be null")
        require(params.nonEmpty, "Config cannot be empty")

        kineticaURL = params.get(KINETICA_URL_PARAM).getOrElse(null) 
        streamURL = params.get(KINETICA_STREAMURL_PARAM).getOrElse(null) 
        kusername = params.get(KINETICA_USERNAME_PARAM).getOrElse("")
        kpassword = params.get(KINETICA_PASSWORD_PARAM).getOrElse("")
        threads =   params.get(KINETICA_NUMTHREADS_PARAM).getOrElse("4").toInt      

        insertSize = params.get(KINETICA_BATCHSIZE_PARAM).getOrElse("10000").toInt
        updateOnExistingPk = params.get(KINETICA_UPDATEONEXISTINGPK_PARAM).getOrElse("false").toBoolean
        tableReplicated = params.get(KINETICA_REPLICATEDTABLE_PARAM).getOrElse("false").toBoolean
        KdbIpRegex = params.get(KINETICA_IPREGEX_PARAM).getOrElse("")
        useSnappy = params.get(KINETICA_USESNAPPY_PARAM).getOrElse("false").toBoolean   
        
        retryCount = params.get(KINETICA_RETRYCOUNT_PARAM).getOrElse("5").toInt    
        jdbcURL = params.get(KINETICA_JDBCURL_PARAM).getOrElse(null)
        createTable = params.get(KINETICA_CREATETABLE_PARAM).getOrElse("false").toBoolean
        
        alterTable = params.get(KINETICA_ALTERTABLE_PARAM).getOrElse("false").toBoolean
        mapToSchema = params.get(KINETICA_MAPTOSCHEMA_PARAM).getOrElse("false").toBoolean

        timeoutMs = params.get(KINETICA_TIMEOUT_PARAM).getOrElse("10000").toInt
        multiHead = params.get(KINETICA_MULTIHEAD_PARAM).getOrElse("false").toBoolean
        
        truncateTable = params.get(KINETICA_TRUNCATETABLE_PARAM).getOrElse("false").toBoolean
        
        loaderPath = params.get(LOADERCODEPATH).getOrElse("false").toBoolean

        tablename = params.get(KINETICA_TABLENAME_PARAM).getOrElse(null)
        if(tablename == null) {
            throw new Exception( "Parameter is required: " + KINETICA_TABLENAME_PARAM)
        }

        if( loaderPath ) {
            val tableParams: Array[String] = tablename.split("\\.")
            if (tableParams.length != 2) {
                throw new Exception( "tablename is needed in the form [schema].[table] " + tablename)
            }                 
            tablename = tableParams(1)
            schemaname = tableParams(0)
        }
    }
}
