package com.kinetica.spark.util.table

import java.io.Serializable
import java.util.ArrayList
import java.util.Iterator
import java.util.List

import com.kinetica.spark.LoaderParams

//remove if not needed
import scala.collection.JavaConversions._
import com.typesafe.scalalogging.LazyLogging

object AlterTableModifyColumnDDL extends LazyLogging {

  private var alterTableDDL: StringBuffer = null

  def init(lp: LoaderParams): Unit = {
    alterTableDDL = new StringBuffer()
      .append("ALTER TABLE " + lp.getTablename + " ALTER COLUMN ")
    //compressDDLs = new ArrayList<String>();
    SubTypeDDL.init()
  }

  /**
    * This method will remove alter table initialization.
    * Used when alter for column is not required
    */
  def deInit(): Unit = {
    alterTableDDL = null
  }

  /**
    * Method will return boolean condition if a alter column needs to be executed
    * @return boolean column alter status
    */
  def columnAlterDetected(): Boolean = {
    if (alterTableDDL != null) {
      true
    }
    false
  }

  def buildNumeric(columnName: String,
                   intType: String,
                   nullable: Boolean): Unit = {
    addToDDL(BuildNumericDDL.buildDDL(columnName, intType), nullable)
  }

  def buildString(columnName: String,
                  maxStringLen: Int,
                  nullable: Boolean): Unit = {
    addToDDL(BuildStringDDL.buildDDL(columnName, maxStringLen), nullable)
  }

  def buildTS(columnName: String, nullable: Boolean): Unit = {
    addToDDL(BuildTSDDL.buildDDL(columnName), nullable)
  }

  def buildDate(columnName: String, nullable: Boolean): Unit = {
    addToDDL(BuildDateDDL.buildDDL(columnName), nullable)
  }

  def buildBoolean(columnName: String, nullable: Boolean): Unit = {
    addToDDL(BuildBooleanDDL.buildDDL(columnName), nullable)
  }

  private def addToDDL(ddlmod: String, nullable: Boolean): Unit = {
    logger.debug("adding to ddl")
    logger.info(ddlmod)
    alterTableDDL.append(ddlmod)
    if (!nullable) {
      ddlmod.concat(" NOT NULL")
    }
  }

  /**
    * This returns only the current alter statement for the column
    * the process is currently working on.  Use with care
    * @return
    */
  def getAlterTableDDL(): String = alterTableDDL.toString

}
