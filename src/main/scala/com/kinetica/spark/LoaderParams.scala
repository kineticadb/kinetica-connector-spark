package com.kinetica.spark

import com.gpudb.Type
import com.gpudb.GPUdb
import com.gpudb.GPUdbBase
import com.gpudb.GenericRecord

import java.io.Serializable
import scala.beans.{ BeanProperty, BooleanBeanProperty }
import org.apache.spark.Logging
import com.kinetica.spark.util.ConfigurationConstants._

import com.kinetica.spark.ssl.X509KeystoreOverride
import com.kinetica.spark.ssl.X509TrustManagerOverride
import com.kinetica.spark.ssl.X509TustManagerBypass

import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.KeyManager
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager

import org.apache.spark.SparkContext
import org.apache.spark.Accumulator

class LoaderParams extends Serializable with Logging {

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

    @BeanProperty
    var bypassCert: Boolean = false

    @BeanProperty
    var trustStorePath: String = null

    @BeanProperty
    var trustStorePassword: String = null

    @BeanProperty
    var keyStorePath: String = null

    @BeanProperty
    var keyStorePassword: String = null

    @BeanProperty
    var truncateToSize: Boolean = false
    
    @BeanProperty
    var dryRun: Boolean = false

    def this(sc: SparkContext, params: Map[String, String]) = {
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
        truncateToSize = params.get(KINETICA_TRUNCATE_TO_SIZE).getOrElse("false").toBoolean

        timeoutMs = params.get(KINETICA_TIMEOUT_PARAM).getOrElse("10000").toInt
        multiHead = params.get(KINETICA_MULTIHEAD_PARAM).getOrElse("false").toBoolean

        truncateTable = params.get(KINETICA_TRUNCATETABLE_PARAM).getOrElse("false").toBoolean

        loaderPath = params.get(LOADERCODEPATH).getOrElse("false").toBoolean
        dryRun = params.get(KINETICA_DRYRUN).getOrElse("false").toBoolean

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

        // SSL
        bypassCert = params.get(KINETICA_SSLBYPASSCERTCJECK_PARAM).getOrElse("false").toBoolean
        trustStorePath =  params.get(KINETICA_TRUSTSTOREJKS_PARAM).getOrElse(null)
        trustStorePassword = params.get(KINETICA_TRUSTSTOREPASSWORD_PARAM).getOrElse(null)
        keyStorePath = params.get(KINETICA_KEYSTOREP12_PARAM).getOrElse(null)
        keyStorePassword = params.get(KINETICA_KEYSTOREPASSWORD_PARAM).getOrElse(null)
    }

    // below are not serializable so they are created on demand
    @transient
    private var cachedGpudb: GPUdb = null

    def getType(): Type = this.tableType

    def setType(kineticaType: Type): Unit = {
        this.tableType = kineticaType
    }

    def getGpudb(): GPUdb = {
        if (this.cachedGpudb != null) {
            this.cachedGpudb
        }
        this.cachedGpudb = connect()
        this.cachedGpudb
    }

    private def connect(): GPUdb = {
        setupSSL()
        logInfo("Connecting to url/username" + kineticaURL + "/" + kusername)
        val opts: GPUdbBase.Options = new GPUdbBase.Options()
        opts.setUsername(kusername)
        opts.setPassword(kpassword)
        opts.setThreadCount(threads)
        opts.setTimeout(timeoutMs)
        opts.setUseSnappy(useSnappy)
        val gpudb: GPUdb = new GPUdb(kineticaURL, opts)
        checkConnection(gpudb)
        gpudb
    }

    private def checkConnection(conn: GPUdb): Unit = {
        val CORE_VERSION: String = "version.gpudb_core_version"
        val VERSION_DATE: String = "version.gpudb_version_date"
        val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
        options.put(CORE_VERSION, "")
        options.put(VERSION_DATE, "")
        val rsMap: java.util.Map[String, String] = conn.showSystemProperties(options).getPropertyMap
        logInfo("Conected to CORE_VERSION/VERSION_DATE)" + rsMap.get(CORE_VERSION) + "/" + rsMap.get(VERSION_DATE))
    }

    def hasTable(): Boolean = {
        val gpudb: GPUdb = this.getGpudb
        if (!gpudb.hasTable(this.tablename, null).getTableExists) {
            false
        } else {
            logInfo("Found existing table: " + this.tablename)
            true
        }
    }

    private def setupSSL(): Unit = {
        if (this.bypassCert) {
            logInfo("Installing truststore to bypass certificate check.")
            X509TustManagerBypass.install()
            return
        }

        var trustManagerList: Array[TrustManager] = null
        if (this.trustStorePath != null) {
            logInfo("Installing custom trust manager: " + classOf[X509TrustManagerOverride].getName)
            trustManagerList = X509TrustManagerOverride.newManagers(
                this.trustStorePath,
                this.trustStorePassword)
        }

        var keyManagerList: Array[KeyManager] = null
        if (this.keyStorePath != null) {
            logInfo("Installing custom key manager: " + classOf[X509KeystoreOverride].getName)
            keyManagerList = X509KeystoreOverride.newManagers(
                this.keyStorePath,
                this.keyStorePassword)
        }

        if (trustManagerList == null && keyManagerList == null) {
            // nothing to install
            return
        }

        val sslContext: SSLContext = SSLContext.getInstance("SSL")
        sslContext.init(keyManagerList, trustManagerList, new java.security.SecureRandom())
        //SSLContext.setDefault(sslContext);
        HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory)
    }
}
