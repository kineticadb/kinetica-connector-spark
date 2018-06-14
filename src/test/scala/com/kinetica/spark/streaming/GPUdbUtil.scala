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

    /*
    def getGPUdb(config: Configuration): GPUdb = {
        val gpudbUrl: String = "http://" + config.get(CONFIG_GPUDB_HOST) + ":" + config
            .get(CONFIG_GPUDB_PORT)
        val gpudb: GPUdb = new GPUdb(gpudbUrl)
        gpudb
    }
		*/
    
    /**
     * Creates a table within GPUdb
     *
     * @param gpudbUrl HTTP address of GPUdb server in which table should be
     *        created
     * @param collectionName name of collection in which to create table
     * @param tableName name of table to create
     * @param tableType class name of table type to create
     * @return true, if table was created successfully;
     *         false, if not because table already exists
     * @throws GPUdbException if table didn't exist and failed to be created
     */
    def createTable(
        gpudbUrl: String,
        collectionName: String,
        tableName: String,
        tableType: Class[_ <: RecordObject]): Boolean = {
        val gpudb: GPUdb = new GPUdb(gpudbUrl)
        createTable(gpudb, collectionName, tableName, tableType)
    }

    /**
     * Creates a table within GPUdb
     *
     * @param gpudb GPUdb connection to use to create table
     * @param collectionName name of collection in which to create table
     * @param tableName name of table to create
     * @param tableType class name of table type to create
     * @return true, if table was created successfully;
     *         false, if not because table already exists
     * @throws GPUdbException if table didn't exist and failed to be created
     */
    def createTable(
        gpudb: GPUdb,
        collectionName: String,
        tableName: String,
        tableType: Class[_ <: RecordObject]): Boolean = {
        var alreadyExists: Boolean = false
        try {
            val typeId: String = RecordObject.createType(tableType, gpudb)
            gpudb.addKnownType(typeId, tableType)
            gpudb.createTable(
                tableName,
                typeId,
                GPUdbBase.options(
                    CreateTableRequest.Options.COLLECTION_NAME,
                    collectionName))
        } catch {
            case e: GPUdbException =>
                if (e.getMessage.contains("already exists")) alreadyExists = true
                else throw e

        }
        !alreadyExists
    }

}
