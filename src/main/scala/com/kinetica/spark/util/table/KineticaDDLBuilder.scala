package com.kinetica.spark.util.table

import java.util.{ ArrayList, List }

import com.kinetica.spark.LoaderParams
import com.typesafe.scalalogging.LazyLogging


object KineticaDDLBuilder extends LazyLogging {

    private var createTableDDL: StringBuffer = null

    private var compressDDLs: List[String] = null

    private var firstColumn: Boolean = true

    def init(lp: LoaderParams): Unit = {
        // Get the table creation part of the DDL
        createTableDDL =
            if (lp.isTableReplicated) {
                // Replicated tables must be explicitly declared
                new StringBuffer().append(
                    "CREATE REPLICATED TABLE " + lp.getTablename +
                        " (")
            }
            else {
                if ( lp.getSchemaname.isEmpty ) {
                    // Regular table
                    new StringBuffer()
                        .append("CREATE TABLE " + lp.getTablename + " (")
                }
                else {
                    // Table with a collection
                    new StringBuffer()
                        .append("CREATE TABLE " + lp.getSchemaname + "." + lp.getTablename + " (")
                }
            }
        compressDDLs = new ArrayList[String]()
        firstColumn = true
        SubTypeDDL.init()
    }

    def buildNumeric(
        columnName: String,
        intType: String,
        nullable: Boolean): Unit = {
        addToDDL(BuildNumericDDL.buildDDL(columnName, intType), nullable)
    }

    def buildString(
        columnName: String,
        maxStringLen: Int,
        nullable: Boolean): Unit = {
        addToDDL(BuildStringDDL.buildDDL(columnName, maxStringLen), nullable)
    }
    
    def buildDecimal(columnName: String, precision: Int, scale: Int, nullable: Boolean): Unit = {
        addToDDL(BuildDecimalDDL.buildDDL(columnName, precision, scale), nullable)
    }

    def buildTS(columnName: String, nullable: Boolean): Unit = {
        addToDDL(BuildTSDDL.buildDDL(columnName), nullable)
    }

    def buildDate(columnName: String, nullable: Boolean): Unit = {
        addToDDL(BuildDateDDL.buildDDL(columnName), nullable)
    }
    
    def buildByteArray(columnName: String, nullable: Boolean): Unit = {
        addToDDL(BuildByteDDL.buildDDL(columnName), nullable)
    }

    def buildBoolean(columnName: String, nullable: Boolean): Unit = {
        addToDDL(BuildBooleanDDL.buildDDL(columnName), nullable)
    }

    def closeDDL(): Unit = {
        createTableDDL.append(SubTypeDDL.getPrimaryKeyDDL)
        createTableDDL.append(" )")
        logger.debug(createTableDDL.toString)
        firstColumn = true
    }

    private def addToDDL(ddlmod: String, nullable: Boolean): Unit = {
        if (!nullable) {
            ddlmod.concat(" NOT NULL")
        }
        if (firstColumn) {
            createTableDDL.append(" " + ddlmod)
            firstColumn = false
        } else {
            createTableDDL.append(", " + ddlmod)
        }
    }

    def getCreateTableDDL(): String = createTableDDL.toString

}
