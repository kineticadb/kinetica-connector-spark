package com.kinetica.spark.util.table

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

//remove if not needed
import scala.collection.JavaConversions._

import com.typesafe.scalalogging.LazyLogging

object ColumnProcessor extends LazyLogging {

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
        ds: DataFrame,
        columnName: String,
        nullable: Boolean,
        alterDDL: Boolean,
        existingColumn: Boolean,
        dryRun : Boolean,
        unrestrictedString : Boolean = false): Unit = {
        var maxInt = 1
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
        ds: DataFrame,
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
        ds: DataFrame,
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
        ds: DataFrame,
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
        ds: DataFrame,
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
