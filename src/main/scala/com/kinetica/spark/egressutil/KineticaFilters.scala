package com.kinetica.spark.egressutil

import java.sql.Timestamp

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.sources._

/**
 * Push down filters to the Kinetica database. Generates LIMIT clause to inject into
 * select statement executed on the Kinetica database. This implementation is based
 * on the spark sql builtin jdbc data source.
 */
private[kinetica] object KineticaFilters {

    /**
     * Returns a WHERE clause representing filter if any
     */
    def getWhereClause(filters: Array[Filter]): String = {
        val filterWhereClause = getFilterClause(filters)
        filterWhereClause
    }

    /**
     * Returns a WHERE clause representing both filter if any, and the current partition LIMIT clause.
     */
    def getWhereClause(filters: Array[Filter], part: KineticaPartition): (String, String) = {
        val filterWhereClause = getFilterClause(filters)
        if (filterWhereClause.length > 0) {
            (filterWhereClause + " LIMIT " + part.startRow + "," + part.numRows, ""+part.numRows)
        } else {
            (" LIMIT " + part.startRow + "," + part.numRows, ""+part.numRows)
        }
    }

    /**
     * Converts filters into a WHERE clause suitable for injection into a Kinetica SQL query.
     */
    def getFilterClause(filters: Array[Filter]): String = {
        val filterStrings = filters map generateFilterExpr filter (_ != null)
        if (filterStrings.size > 0) {
            val sb = new StringBuilder("WHERE ")
            filterStrings.foreach(x => sb.append(x).append(" AND "))
            sb.substring(0, sb.length - 5)
        } else ""
    }

    /**
     * Convert the given String into a quotes SQL string value.
     */
    private def quoteValue(value: Any): Any = value match {
        case stringValue: String => s"'${escapeQuotes(stringValue)}'"
        case tsValue: Timestamp => s"'${tsValue}'"
        case _ => value
    }

    /**
     * Return a strings that escapes quote with another quote.
     */
    private def escapeQuotes(value: String): String =
        if (value == null) null else StringUtils.replace(value, "'", "''")

    /**
     * Turns a single Filter into a String representing a SQL expression.
     * Returns null for an unhandled filter.
     */
    private def generateFilterExpr(f: Filter): String = f match {
        case EqualTo(attr, value) => s"$attr = ${quoteValue(value)}"
        case LessThan(attr, value) => s"$attr < ${quoteValue(value)}"
        case GreaterThan(attr, value) => s"$attr > ${quoteValue(value)}"
        case LessThanOrEqual(attr, value) => s"$attr <= ${quoteValue(value)}"
        case GreaterThanOrEqual(attr, value) => s"$attr >= ${quoteValue(value)}"
        case IsNull(attr) => s"$attr is null"
        case IsNotNull(attr) => s"$attr is not null"
        case _ => null
    }
}
