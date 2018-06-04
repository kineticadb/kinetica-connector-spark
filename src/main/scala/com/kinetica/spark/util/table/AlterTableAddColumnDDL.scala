package com.kinetica.spark.util.table

import com.typesafe.scalalogging.LazyLogging

//remove if not needed
import scala.collection.JavaConversions._
import com.kinetica.spark.LoaderParams

object AlterTableAddColumnDDL extends LazyLogging {

  private var alterTableDDL: StringBuffer = null

  def init(lp: LoaderParams): Unit = {
    alterTableDDL =
      new StringBuffer().append("ALTER TABLE " + lp.getTablename + " ADD ")
//  compressDDLs = new ArrayList<String>();
    SubTypeDDL.init()
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
    * @return alter ddl
    */
  def getAlterTableDDL(): String = alterTableDDL.toString

}
