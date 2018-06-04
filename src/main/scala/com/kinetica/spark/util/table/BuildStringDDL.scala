package com.kinetica.spark.util.table

//remove if not needed
import scala.collection.JavaConversions._

object BuildStringDDL {

  def buildDDL(columnName: String, maxStringLen: Int): String =
    if (SubTypeDDL.isWKT(columnName)) {
      columnName + " STRING(WKT) "
    } else {
      val inLineSub: String = SubTypeDDL.buildInLineSubType(columnName)
      if (SubTypeDDL.isStringType(columnName)) {
        if (inLineSub.trim().length > 0) {
          columnName + " STRING(" + inLineSub + ")" + SubTypeDDL
            .buildOutLineSubType(columnName)
        } else {
          columnName + " STRING " + SubTypeDDL.buildOutLineSubType(columnName)
        }
      } else {
        if (inLineSub.trim().length > 0) {
          columnName + " VARCHAR(" + maxStringLen + " , " + inLineSub +
            ")" +
            SubTypeDDL.buildOutLineSubType(columnName)
        } else {
          columnName + " VARCHAR(" + maxStringLen + ")" + SubTypeDDL
            .buildOutLineSubType(columnName)
        }
      }
    }

}
