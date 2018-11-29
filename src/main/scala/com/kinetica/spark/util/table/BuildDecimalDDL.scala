package com.kinetica.spark.util.table

//remove if not needed
import scala.collection.JavaConversions._

object BuildDecimalDDL {

  def buildDDL(columnName: String, precision: Int, scale: Int): String = {
    val inLineSub: String = SubTypeDDL.buildInLineSubType(columnName)
    if (inLineSub.trim().length > 0) {
      columnName + " DECIMAL(" + precision + "," + scale + inLineSub + ")" + SubTypeDDL
        .buildOutLineSubType(columnName)
    } else {
      columnName + " DECIMAL(" + precision + "," + scale + ")" + SubTypeDDL.buildOutLineSubType(
        columnName)
    }
  }
}