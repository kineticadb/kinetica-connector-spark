package com.kinetica.spark.util.table

object BuildNumericDDL {

  def buildDDL(columnName: String, intType: String): String = {
    val inLineSub: String = SubTypeDDL.buildInLineSubType(columnName)
    
    //println(" ##### BuildNumericDDL buildDDL intType is " + intType + " inlinesub is " + inLineSub)
    //println(" ##### BuildNumericDDL columnName " + columnName)
    
    if (intType.compareToIgnoreCase("int8") == 0) {
      if (inLineSub.trim().length > 0) {
        return columnName + " DECIMAL(2,0, " + inLineSub + ")" + SubTypeDDL
          .buildOutLineSubType(columnName)
      } else {
        return columnName + " DECIMAL(2,0) " + SubTypeDDL.buildOutLineSubType(
          columnName)
      }
    } else if (intType.compareToIgnoreCase("int16") == 0) {
      if (inLineSub.trim().length > 0) {
        return columnName + " DECIMAL(4,0, " + inLineSub + ")" + SubTypeDDL
          .buildOutLineSubType(columnName)
      } else {
        return columnName + " DECIMAL(4,0) " + SubTypeDDL.buildOutLineSubType(
          columnName)
      }
    } else if (intType.compareToIgnoreCase("integer") == 0) {
      if (inLineSub.trim().length > 0) {
        return columnName + " INTEGER( " + inLineSub + ")" + SubTypeDDL
          .buildOutLineSubType(columnName)
      } else {
        return columnName + " INTEGER " + SubTypeDDL.buildOutLineSubType(columnName)
      }
    } else if (intType.compareToIgnoreCase("long") == 0) {
      if (inLineSub.trim().length > 0) {
        return  columnName + " LONG( " + inLineSub + ")" + SubTypeDDL
          .buildOutLineSubType(columnName)
      } else {
        return columnName + " LONG " + SubTypeDDL.buildOutLineSubType(columnName)
      }
    } else if (intType.compareToIgnoreCase("decimal") == 0) {
      if (inLineSub.trim().length > 0) {
        return columnName + " DECIMAL(10,10, " + inLineSub + ")" + SubTypeDDL
          .buildOutLineSubType(columnName)
      } else {
        return columnName + " DECIMAL(10,10) " + SubTypeDDL.buildOutLineSubType(
          columnName)
      }
    } else if (intType.compareToIgnoreCase("float") == 0) {
      if (inLineSub.trim().length > 0) {
        return columnName + " FLOAT( " + inLineSub + ")" + SubTypeDDL
          .buildOutLineSubType(columnName)
      } else {
        return columnName + " FLOAT " + SubTypeDDL.buildOutLineSubType(columnName)
      }
    } else if (intType.compareToIgnoreCase("double") == 0) {
      if (inLineSub.trim().length > 0) {
        return columnName + " DOUBLE( " + inLineSub + ")" + SubTypeDDL
          .buildOutLineSubType(columnName)
      } else {
        return columnName + " DOUBLE " + SubTypeDDL.buildOutLineSubType(columnName)
      }
    } else {
        return null
    }
  }
}
