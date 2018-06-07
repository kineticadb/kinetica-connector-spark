package com.kinetica.spark.util

import com.gpudb._
import java.net.URL
import java.util.HashMap
import java.util.Iterator
import java.util.Map
import java.util.regex.Pattern
import KineticaBulkLoader._
import scala.beans.{ BeanProperty, BooleanBeanProperty }
import scala.collection.JavaConversions._
import com.typesafe.scalalogging.LazyLogging
import com.kinetica.spark.LoaderParams

object KineticaBulkLoader {

}

class KineticaBulkLoader(bkp: LoaderParams) extends LazyLogging {
    val lp = bkp
    def GetBulkInserter(): BulkInserter[GenericRecord] = {
        var gpudb: GPUdb = null
        try {
            logger.debug("connecting to: " + lp.kineticaURL, getGPUDBOptions)
            gpudb = new GPUdb(lp.kineticaURL, getGPUDBOptions)
        } catch {
            case e: Exception => {
                e.printStackTrace()
                logger.error("failure:", e)
            }

        }
        var bulkInserter: BulkInserter[GenericRecord] = null

        if (lp.isTableReplicated) {
            logger.info("Table is set to Is Replication: True")
            try {
                bulkInserter = new BulkInserter[GenericRecord](
                    gpudb,
                    lp.tablename,
                    lp.tableType,
                    lp.insertSize,
                    getUpsertOptions)
                bulkInserter.setRetryCount(lp.retryCount)
            } catch {
                case e: GPUdbException => {
                    e.printStackTrace()
                    logger.error("failure:", e)
                }

            }
        } else {
            try {
                logger.info("Table is not replicated table")
                val workers: BulkInserter.WorkerList = getWorkers(gpudb)
                if (workers != null) {
                    logger.info("multi-head ingest turned on")
                    bulkInserter = new BulkInserter[GenericRecord](
                        gpudb,
                        lp.tablename,
                        lp.tableType,
                        lp.insertSize,
                        getUpsertOptions,
                        workers)
                } else {
                    logger.info("Multi-head ingest is turned off")
                    bulkInserter = new BulkInserter[GenericRecord](
                        gpudb,
                        lp.tablename,
                        lp.tableType,
                        lp.insertSize,
                        getUpsertOptions)
                }
                bulkInserter.setRetryCount(lp.retryCount)
            } catch {
                case e: GPUdbException => {
                    e.printStackTrace()
                    logger.error("failure:", e)
                }

            }
        }
        bulkInserter
    }

    private def getUpsertOptions(): Map[String, String] = {
        val options: Map[String, String] = new HashMap[String, String]()
        options.put(
            "update_on_existing_pk",
            java.lang.Boolean.toString(lp.updateOnExistingPk))
        options
    }

    private def getWorkers(gpudb: GPUdb): BulkInserter.WorkerList = {
        var workers: BulkInserter.WorkerList = null
        if ((lp.KdbIpRegex != null) && !(lp.KdbIpRegex.trim().equalsIgnoreCase(""))) {
            logger.debug("gpudbIpRegex not null: " + lp.KdbIpRegex)
            val pattern: Pattern = Pattern.compile(lp.KdbIpRegex)
            try workers = new BulkInserter.WorkerList(gpudb, pattern)
            catch {
                case e: Exception => {
                    e.printStackTrace()
                    logger.error("failure:", e)
                }

            }
        } else {
            try workers = new BulkInserter.WorkerList(gpudb)
            catch {
                case e: Exception => {
                    e.printStackTrace()
                    logger.error("failure:", e)
                }

            }
        }
        if (workers != null) {
            logger.debug("Number of workers: " + workers.size)
            var iter: Iterator[URL] = workers.iterator()
            while (iter.hasNext) logger.debug(
                "GPUdb BulkInserter worker: " + iter.next())
            workers
        } else {
            logger.info("No workers available. Multi-head ingest may be turned off")
            null
        }
    }

    /**
     * WritenBy - sunman
     * 9/2/2018
     * @return GPUdbBase.Options
     */
    private def getGPUDBOptions(): GPUdbBase.Options = {
        val opts: GPUdbBase.Options = new GPUdbBase.Options()
        logger.debug("Authentication enabled: " + lp.kauth)
        if (lp.kauth) {
            logger.debug("Setting username and password")
            opts.setUsername(lp.kusername.trim())
            opts.setPassword(lp.kpassword.trim())
        }
        opts.setThreadCount(lp.getThreads())
        opts.setUseSnappy(lp.useSnappy)
        opts
    }
}
