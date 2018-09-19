package com.kinetica.spark.util.table

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

//remove if not needed
import scala.collection.JavaConversions._

import org.apache.spark.Logging

object ColumnProcessor extends Logging {

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
        } else if (dt.isInstanceOf[DecimalType]) {
            processNumeric(columnName, "decimal", nullable, alterDDL)
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
        dryRun : Boolean): Unit = {
        var maxIntDs: DataFrame = null
        maxIntDs = TypeStringProcessor.getMaxStringLen(ds, columnName)
        //set to 0 for new columns
        var existingColumnMaxLength: Int = 0
        
        var maxInt = 1
        try {
            maxInt = maxIntDs.first.getInt(0)
        } catch {
            case e: Exception => {
                logInfo("Could not determine max length for column, setting to 1 " + columnName)
            }
        }
        
        logInfo(" @@@@@@@@@@@@ column name and max length is " + columnName + "/" + maxInt)

        try if (alterDDL && existingColumn) {
            existingColumnMaxLength =
                SparkKineticaTableUtil.getExistingColumnCharN(columnName)
        } catch {
            case e: KineticaException => existingColumnMaxLength = -1

            case e: Exception => {
                logDebug("Parse error, skipping column")
                existingColumnMaxLength = -1
            }

        }
        if (alterDDL) {
            //if new column
            if (!existingColumn) {
                AlterTableAddColumnDDL.buildString(columnName, maxInt, nullable)
            } else //existing column
            if (maxInt > existingColumnMaxLength && existingColumnMaxLength >= 0) {
                AlterTableModifyColumnDDL.buildString(columnName, maxInt, nullable)
            } else {
                //remove alter table modification init alter statement
                AlterTableModifyColumnDDL.deInit()
            }
        } else {
            //new table
            KineticaDDLBuilder.buildString(columnName, maxInt, nullable)
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
