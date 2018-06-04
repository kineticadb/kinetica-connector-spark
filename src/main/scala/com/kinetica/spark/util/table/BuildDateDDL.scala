package com.kinetica.spark.util.table

//remove if not needed
import scala.collection.JavaConversions._

object BuildDateDDL {
  def buildDDL(columnName: String): String = {
    val inLineSub: String = SubTypeDDL.buildInLineSubType(columnName)
    if (inLineSub.trim().length > 0) {
      columnName + " DATE(" + inLineSub + ")" + SubTypeDDL.buildOutLineSubType(
        columnName)
    } else {
      columnName + " DATE " + SubTypeDDL.buildOutLineSubType(columnName)
    }
  }
}
