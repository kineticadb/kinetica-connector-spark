package com.kinetica.spark.util.table

import com.typesafe.scalalogging.LazyLogging

object BuildByteDDL extends Serializable with LazyLogging {
    def buildDDL(columnName: String): String = {
        columnName + " BYTES(STORE_ONLY) " + SubTypeDDL.buildOutLineSubType(
                columnName)
    }
}