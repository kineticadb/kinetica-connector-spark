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
     * Returns a tuple where the first element is the WHERE clause representing
     * the filters and the offset & limit clause, and the second element is the limit.
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
     * Returns the WHERE clause representing the filters and the offset & limit
     * clause, and the second element is the limit.
     */
    def getWhereClause( filters: Array[Filter],
                        offset: Long,
                        limit: Long ): String = {
        val filterWhereClause = getFilterClause(filters)
        if (filterWhereClause.length > 0) {
            s"$filterWhereClause LIMIT $offset, $limit"
        } else {
            s" LIMIT $offset, $limit"
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
        if (value == null) null else StringUtils.replace(value, "'", "''");

    /**
     * Turns a single Filter into a String representing a SQL expression.
     * Returns null for an unhandled filter.
     */
    private def generateFilterExpr(f: Filter): String = {
        f match {
            case EqualTo(field, value) => s"$field = ${quoteValue(value)}"
            case LessThan(field, value) => s"$field < ${quoteValue(value)}"
            case GreaterThan(field, value) => s"$field > ${quoteValue(value)}"
            case LessThanOrEqual(field, value) => s"$field <= ${quoteValue(value)}"
            case GreaterThanOrEqual(field, value) => s"$field >= ${quoteValue(value)}"
            case IsNull(field) => s"$field is null"
            case IsNotNull(field) => s"$field is not null"
            case _ => null
            // May want to also support these filters in the future
            // case EqualNullSafe(field, value)
            // case In(field, values)
            // case And(leftFilter, rightFilter)
            // case Or(leftFilter, rightFilter)
            // case Not(filter)
            // case StringStartsWith(field, value)
            // case StringEndsWith(field, value)
            // case StringContains(field, value)
        }
    }
}
