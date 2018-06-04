package com.kinetica.spark.loader

import java.io.Serializable

import scala.beans.BeanProperty
import scala.collection.JavaConversions.asScalaBuffer

import com.gpudb.BulkInserter
import com.gpudb.GPUdb
import com.gpudb.GPUdbBase
import com.gpudb.GenericRecord
import com.gpudb.Type
import com.gpudb.protocol.InsertRecordsRequest
import com.kinetica.spark.LoaderParams
import com.kinetica.spark.ssl.X509KeystoreOverride
import com.kinetica.spark.ssl.X509TrustManagerOverride
import com.kinetica.spark.ssl.X509TustManagerBypass
import com.kinetica.spark.util.ConfigurationConstants._
import com.typesafe.scalalogging.LazyLogging

import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.KeyManager
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager

@SerialVersionUID(-2502861044221136156L)
class LoaderConfiguration(params: Map[String, String]) extends LoaderParams(params) with Serializable with LazyLogging {

    @BeanProperty
    val multiHead: Boolean = params.get(KINETICA_MULTIHEAD_PARAM).getOrElse("false").toBoolean

    @BeanProperty
    val truncateTable: Boolean = params.get(KINETICA_TRUNCATETABLE_PARAM).getOrElse("false").toBoolean

    @BeanProperty
    val sqlFileName: String = params.get(CONNECTOR_SQLFILE_PARAM).getOrElse(null)

    @BeanProperty
    val dataPath: String = params.get(CONNECTOR_DATAPATH_PARAM).getOrElse(null)

    @BeanProperty
    val dataFormat: String = params.get(CONNECTOR_DATAFORMAT_PARAM).getOrElse(null)

    @BeanProperty
    val useTemplates: Boolean = params.get(KINETICA_USETEMPLATES_PARAM).getOrElse("false").toBoolean

    @BeanProperty
    val timeoutMs: Int = params.get(KINETICA_TIMEOUT_PARAM).getOrElse("10000").toInt

    @BeanProperty
    val partitionRows: Int = params.get(KINETICA_PARTROWS_PARAM).getOrElse("-1").toInt

    if(tablename == null) {
        throw new Exception( "Parameter is required: " + KINETICA_TABLENAME_PARAM)
    }

    private val tableParams: Array[String] = tablename.split("\\.")
    if (tableParams.length != 2) {
        throw new Exception( "loader.dest-table must have [schema].[table]: " + tablename)
    }

    val tableName: String = tableParams(1)
    val schemaName: String = tableParams(0)

    // SSL
    private val bypassCert: Boolean =
        params.get(KINETICA_SSLBYPASSCERTCJECK_PARAM).getOrElse("false").toBoolean

    private val trustStorePath: String =
        params.get(KINETICA_TRUSTSTOREJKS_PARAM).getOrElse(null)

    private val trustStorePassword: String =
        params.get(KINETICA_TRUSTSTOREPASSWORD_PARAM).getOrElse(null)

    private val keyStorePath: String =
        params.get(KINETICA_KEYSTOREP12_PARAM).getOrElse(null)

    private val keyStorePassword: String =
        params.get(KINETICA_KEYSTOREPASSWORD_PARAM).getOrElse(null)

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
        logger.info("Connecting to {} as <{}>", kineticaURL, kusername)
        val opts: GPUdbBase.Options = new GPUdbBase.Options()
        opts.setUsername(kusername)
        opts.setPassword(kpassword)
        opts.setThreadCount(threads)
        opts.setTimeout(timeoutMs)
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
        logger.info("Conected to {} ({})", rsMap.get(CORE_VERSION), rsMap.get(VERSION_DATE))
    }

    def hasTable(): Boolean = {
        val gpudb: GPUdb = this.getGpudb
        if (!gpudb.hasTable(this.tableName, null).getTableExists) {
            false
        } else {
            logger.info("Found existing table: {}", this.tableName)
            true
        }
    }

    def getBulkInserter(): BulkInserter[GenericRecord] = {
        val tableType: Type = getType
        val gpudb: GPUdb = getGpudb
        val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
        if (updateOnExistingPk) {
            options.put(
                InsertRecordsRequest.Options.UPDATE_ON_EXISTING_PK,
                InsertRecordsRequest.Options.TRUE)
        }
        val workers: BulkInserter.WorkerList = getWorkers
        val bi: BulkInserter[GenericRecord] = new BulkInserter[GenericRecord](
                    gpudb, this.tableName, tableType, insertSize, options, workers)
        bi
    }

    def getWorkers(): BulkInserter.WorkerList = {
        val gpudb: GPUdb = getGpudb
        if (!this.multiHead) {
            logger.info("gpudb.multi-head option is false")
            null
        } else {
            val workers: BulkInserter.WorkerList = new BulkInserter.WorkerList(gpudb)
            if (workers.size == 0) {
                throw new Exception("No gpudb workers found. Multi-head ingest might be disabled.")
            }
            for (worker <- workers) {
                logger.info("GPUdb BulkInserter worker: {}", worker)
            }
            workers
        }
    }

    private def setupSSL(): Unit = {
        if (this.bypassCert) {
            logger.info("Installing truststore to bypass certificate check.")
            X509TustManagerBypass.install()
            return
        }

        var trustManagerList: Array[TrustManager] = null
        if (this.trustStorePath != null) {
            logger.info("Installing custom trust manager: {}", classOf[X509TrustManagerOverride].getName)
            trustManagerList = X509TrustManagerOverride.newManagers(
                this.trustStorePath,
                this.trustStorePassword)
        }

        var keyManagerList: Array[KeyManager] = null
        if (this.keyStorePath != null) {
            logger.info("Installing custom key manager: {}", classOf[X509KeystoreOverride].getName)
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