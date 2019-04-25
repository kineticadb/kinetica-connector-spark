package com.kinetica.spark.util.table

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

//remove if not needed
import scala.collection.JavaConversions._

import com.typesafe.scalalogging.LazyLogging

object ColumnProcessor extends LazyLogging {

    /*
     * Given a column name, surround it by double quotes and
     * return it.
     */
    def quoteColumnName( columnName: String ) : String = {
        return ("\"" + columnName + "\"")
    }
    
    /*
     * Given two column names, compare whether they are equal.  This
     * is a case INsensitive comparison.  It also ignores any quotation
     * around the column names.
     */
    def areColumnNamesEqual( colName1: String, colName2: String ) : Boolean = {

        // Strip '"' from either end, IFF existing at both ends
        val quote = "\""
        var cName1 = colName1
        var cName2 = colName2
        if ( colName1.nonEmpty &&
             (colName1.charAt( 0 ) == colName1.charAt( colName1.length() - 1 ) ) ) {
            cName1 = colName1.stripPrefix( quote ).stripSuffix( quote )
        }
        if ( colName2.nonEmpty &&
             (colName2.charAt( 0 ) == colName2.charAt( colName2.length() - 1 ) ) ) {
            cName2 = colName2.stripPrefix( quote ).stripSuffix( quote )
        }

        // Case insensitive comparison
        return ( cName1.compareToIgnoreCase( cName2 ) == 0 )
    }
    
    def processNumeric(
        dt: DataType,
        columnName: String,
        nullable: Boolean,
        alterDDL: Boolean): Unit = {
        if (dt.isInstanceOf[ByteType]) {
            processNumeric(columnName, "int8", nullable, alterDDL)
        } else if (dt.isInstanceOf[ShortType]) {
            processNumeric(columnName, "int16", nullable, alterDDL)
        } else if (dt.isInstanceOf[IntegerType]) {
            processNumeric(columnName, "integer", nullable, alterDDL)
        } else if (dt.isInstanceOf[LongType]) {
            processNumeric(columnName, "long", nullable, alterDDL)
        } else if (dt.isInstanceOf[FloatType]) {
            processNumeric(columnName, "float", nullable, alterDDL)
        } else if (dt.isInstanceOf[DoubleType]) {
            processNumeric(columnName, "double", nullable, alterDDL)
        } else {
            processNumeric(columnName, "double", nullable, alterDDL)
        }
    }

    private def processNumeric(
        columnName: String,
        intType: String,
        nullable: Boolean,
        alterDDL: Boolean): Unit = {
        if (alterDDL) {
            AlterTableAddColumnDDL.buildNumeric(columnName, intType, nullable)
        } else {
            KineticaDDLBuilder.buildNumeric(columnName, intType, nullable)
        }
    }

    def processString(
        ds: Option[DataFrame],
        columnName: String,
        nullable: Boolean,
        alterDDL: Boolean,
        existingColumn: Boolean,
        dryRun : Boolean,
        unrestrictedString : Boolean = false): Unit = {
        var maxInt = 1
        ds match {
            case Some(ds) => {
                if( !unrestrictedString ) {
                    var maxIntDs: DataFrame = null
                    maxIntDs = TypeStringProcessor.getMaxStringLen(ds, columnName)
                    //set to 0 for new columns
                    var existingColumnMaxLength: Int = 0
                    try {
                        maxInt = maxIntDs.first.getInt(0)
                    } catch {
                        case e: Exception => {
                            logger.info("Could not determine max length for column, setting to 1 " + columnName)
                        }
                    }
                } else {
                    maxInt = 1000; // We need an unrestricted string. 
                }
            }
            case None => maxInt = 1000; // We need an unrestricted string. 
        }
        logger.info(" @@@@@@@@@@@@ column name and max length is " + columnName + "/" + maxInt)
        SparkKineticaTableUtil.charColumnLengths += (columnName -> maxInt)
        KineticaDDLBuilder.buildString(columnName, maxInt, nullable)
    }
    
    def processDecimal(
        dt: DataType,
        columnName: String,
        nullable: Boolean,
        alterDDL: Boolean): Unit = {
        if (alterDDL) {
            // DONT DO ANYTHING - AlterTableAddColumnDDL.buildTS(columnName, nullable)
        } else {
            val decimalType : DecimalType = dt.asInstanceOf[DecimalType]
            KineticaDDLBuilder.buildDecimal(columnName, decimalType.precision, decimalType.scale, nullable)
        }
    }

    def processTS(
        ds: Option[DataFrame],
        columnName: String,
        nullable: Boolean,
        alterDDL: Boolean): Unit = {
        if (alterDDL) {
            AlterTableAddColumnDDL.buildTS(columnName, nullable)
        } else {
            KineticaDDLBuilder.buildTS(columnName, nullable)
        }
    }

    def processDate(
        ds: Option[DataFrame],
        columnName: String,
        nullable: Boolean,
        alterDDL: Boolean): Unit = {
        if (alterDDL) {
            AlterTableAddColumnDDL.buildDate(columnName, nullable)
        } else {
            KineticaDDLBuilder.buildDate(columnName, nullable)
        }
    }
    
    def processByteArray(
        ds: Option[DataFrame],
        columnName: String,
        nullable: Boolean,
        alterDDL: Boolean): Unit = {
        if (alterDDL) {
            //AlterTableAddColumnDDL.buildDate(columnName, nullable)
        } else {
            KineticaDDLBuilder.buildByteArray(columnName, nullable)
        }
    }

    def processBoolean(
        ds: Option[DataFrame],
        columnName: String,
        nullable: Boolean,
        alterDDL: Boolean): Unit = {
        if (alterDDL) {
            AlterTableAddColumnDDL.buildBoolean(columnName, nullable)
        } else {
            KineticaDDLBuilder.buildBoolean(columnName, nullable)
        }
    }

}
