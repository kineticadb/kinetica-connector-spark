package com.kinetica.spark.util.table

//remove if not needed
import scala.collection.JavaConversions._

object BuildTSDDL {

  def buildDDL(columnName: String): String = {
    val inLineSub: String = SubTypeDDL.buildInLineSubType(columnName)
    if (inLineSub.trim().length > 0) {
      columnName + " TYPE_TIMESTAMP(" + inLineSub + ")" + SubTypeDDL
        .buildOutLineSubType(columnName)
    } else {
      columnName + " TYPE_TIMESTAMP " + SubTypeDDL.buildOutLineSubType(
        columnName)
    }
  }
}