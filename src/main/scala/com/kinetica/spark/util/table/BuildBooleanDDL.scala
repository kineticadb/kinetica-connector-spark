package com.kinetica.spark.util.table

import scala.collection.JavaConversions._

object BuildBooleanDDL {

  def buildDDL(columnName: String): String = {
    val inLineSub: String = SubTypeDDL.buildInLineSubType(columnName)
    if (inLineSub.trim().length > 0) {
      columnName + " DECIMAL(1,0 " + inLineSub + ")" + SubTypeDDL
        .buildOutLineSubType(columnName)
    } else {
      columnName + " DECIMAL(1,0) " + SubTypeDDL.buildOutLineSubType(
        columnName)
    }
  }
}