package com.kinetica.spark.util.table

import java.util.Iterator

import com.kinetica.spark.util.table._

object SubTypeDDL {

    private var primarykeyddl: StringBuilder = new StringBuilder()

    private var primarykeyfound: Boolean = false

    def init(): Unit = {
        primarykeyddl = new StringBuilder()
        primarykeyfound = false
    }

    def isWKT(columnName: String): Boolean = {
        var isfound: Boolean = false
        val iterator: Iterator[String] =
        SparkKineticaTableUtil.getWktfields.iterator()
        while (iterator.hasNext) {
            if ( ColumnProcessor.areColumnNamesEqual( iterator.next(),
                                                      columnName ) ) {
                isfound = true
                //break
            }
        }
        isfound
    }

    def isStorOnly(columnName: String): Boolean = {
        var isfound: Boolean = false
        val iterator: Iterator[String] =
        SparkKineticaTableUtil.getStoreonlyfields.iterator()
        while (iterator.hasNext) {
            if ( ColumnProcessor.areColumnNamesEqual( iterator.next(),
                                                      columnName ) ) {
                isfound = true
                //break
            }
        }
        isfound
    }

    def isShardKey(columnName: String): Boolean = {
        var isfound: Boolean = false
        val iterator: Iterator[String] =
        SparkKineticaTableUtil.getShardkeys.iterator()
        while (iterator.hasNext) {
            if ( ColumnProcessor.areColumnNamesEqual( iterator.next(),
                                                      columnName ) ) {
                isfound = true
                //break
            }
        }
        isfound
    }

    def isStringType(columnName: String): Boolean = {
        var isfound: Boolean = false
        val iterator: Iterator[String] =
        SparkKineticaTableUtil.getStringfields.iterator()
        while (iterator.hasNext) {
            if ( ColumnProcessor.areColumnNamesEqual( iterator.next(),
                                                      columnName ) ) {
                isfound = true
                //break
            }
        }
        isfound
    }

    def isDiskOptimized(columnName: String): Boolean = {
        var isfound: Boolean = false
        val iterator: Iterator[String] =
        SparkKineticaTableUtil.getDiskoptimizedfields.iterator()
        while (iterator.hasNext) {
            if ( ColumnProcessor.areColumnNamesEqual( iterator.next(),
                                                      columnName ) ) {
                isfound = true
                //break
            }
        }
        isfound
    }

    def isDictEnoding(columnName: String): Boolean = {
        var isfound: Boolean = false
        val iterator: Iterator[String] =
        SparkKineticaTableUtil.getDictencodingfields.iterator()
        while (iterator.hasNext) {
            if ( ColumnProcessor.areColumnNamesEqual( iterator.next(),
                                                      columnName ) ) {
                isfound = true
                //break
            }
        }
        isfound
    }

    def isTextSearch(columnName: String): Boolean = {
        var isfound: Boolean = false
        val iterator: Iterator[String] =
        SparkKineticaTableUtil.getTextsearchfields.iterator()
        while (iterator.hasNext) {
            if ( ColumnProcessor.areColumnNamesEqual( iterator.next(),
                                                      columnName ) ) {
                isfound = true
                //break
            }
        }
        isfound
    }

  def isIPV4(columnName: String): Boolean = {
      var isfound: Boolean = false
      val iterator: Iterator[String] =
      SparkKineticaTableUtil.getIpv4fields.iterator()
      while (iterator.hasNext) {
          if ( ColumnProcessor.areColumnNamesEqual( iterator.next(),
                                                    columnName ) ) {
              isfound = true
              //break
          }
      }
      isfound
  }

  def isNotNUll(columnName: String): Boolean = {
      var isfound: Boolean = false
      val iterator: Iterator[String] =
      SparkKineticaTableUtil.getNotnullfields.iterator()
      while (iterator.hasNext) {
          if ( ColumnProcessor.areColumnNamesEqual( iterator.next(),
                                                    columnName ) ) {
              isfound = true
              //break
          }
      }
      isfound
  }

    def isPrimaryKey(columnName: String): Boolean = {
        var isfound: Boolean = false
        val iterator: Iterator[String] =
        SparkKineticaTableUtil.getPrimarykeys.iterator()
        while (iterator.hasNext) {
            if ( ColumnProcessor.areColumnNamesEqual( iterator.next(),
                                                      columnName ) ) {
                isfound = true
                //break
            }
        }
        isfound
    }

    def buildInLineSubType(columnName: String): String = {
        val subtypeString: StringBuilder = new StringBuilder()
        var subtypefound: Boolean = false
        if (isStorOnly(columnName)) {
            if (!subtypefound) {
                subtypeString.append(" STORE_ONLY ")
                subtypefound = true
            } else {
                subtypeString.append(",STORE_ONLY ")
            }
        }
        if (isIPV4(columnName)) {
            if (!subtypefound) {
                subtypeString.append(" IPV4 ")
                subtypefound = true
            } else {
                subtypeString.append(",IPV4 ")
            }
        }
        if (isShardKey(columnName)) {
            if (!subtypefound) {
                subtypeString.append(" SHARD_KEY ")
                subtypefound = true
            } else {
                subtypeString.append(",SHARD_KEY ")
            }
        }
        if (isDiskOptimized(columnName)) {
            if (!subtypefound) {
                subtypeString.append(" DISK_OPTIMIZED ")
                subtypefound = true
            } else {
                subtypeString.append(",DISK_OPTIMIZED ")
            }
        }
        if (isDictEnoding(columnName) || SparkKineticaTableUtil.isAllStringDict) {
            if (!subtypefound) {
                subtypeString.append(" DICT ")
                subtypefound = true
            } else {
                subtypeString.append(",DICT ")
            }
        }
        if (isTextSearch(columnName)) {
            if (!subtypefound) {
                subtypeString.append(" TEXT_SEARCH ")
                subtypefound = true
            } else {
                subtypeString.append(",TEXT_SEARCH ")
            }
        }
        if (isPrimaryKey(columnName)) {
            if (!primarykeyfound) {
                primarykeyddl.append(" , PRIMARY KEY ( ")
                primarykeyddl.append(columnName + " ")
                primarykeyfound = true
            } else {
                primarykeyddl.append(" , " + columnName + " ")
            }
        }
        subtypeString.toString
    }

    def buildOutLineSubType(columnName: String): String = 
        if (isNotNUll(columnName) || isPrimaryKey(columnName) ||
            SparkKineticaTableUtil.isAllnotnull) {
            " NOT NULL"
        } else {
            ""
        }

    def getPrimaryKeyDDL(): String =
        if (primarykeyfound) {
            primarykeyddl.append(" )").toString
        } else {
            ""
        }

}
