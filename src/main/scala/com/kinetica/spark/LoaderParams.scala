package com.kinetica.spark

import com.gpudb.Type
import java.io.Serializable
import scala.beans.{ BeanProperty, BooleanBeanProperty }
import com.typesafe.scalalogging.LazyLogging
import com.kinetica.spark.util.ConfigurationConstants._

class LoaderParams extends Serializable with LazyLogging {
    
    @BeanProperty
    var kineticaURL: String = null

    @BeanProperty
    var tableType: Type = null

    @BeanProperty
    var tablename: String = null

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

    def this(params: Map[String, String]) = {
        this()
        require(params != null, "Config cannot be null")
        require(params.nonEmpty, "Config cannot be empty")

        kineticaURL = params.get(KINETICA_URL_PARAM).getOrElse(null) 
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
        tablename = params.get(KINETICA_TABLENAME_PARAM).getOrElse(null)
        createTable = params.get(KINETICA_CREATETABLE_PARAM).getOrElse("false").toBoolean
        
        this.alterTable = params.get(KINETICA_ALTERTABLE_PARAM).getOrElse("false").toBoolean
        this.mapToSchema = params.get(KINETICA_MAPTOSCHEMA_PARAM).getOrElse("false").toBoolean

    }
}
