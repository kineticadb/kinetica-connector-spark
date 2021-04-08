package com.kinetica.spark.datasourcev2;

import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources._; // For all the filters
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import com.kinetica.spark.LoaderParams;
import com.kinetica.spark.egressutil.KineticaEgressUtilsNativeClient;
import com.kinetica.spark.egressutil.KineticaEgressUtilsJdbc;
import com.kinetica.spark.egressutil.KineticaFilters;
import com.kinetica.spark.egressutil.KineticaJdbcUtils;
import com.kinetica.spark.util.ConfigurationConstants._;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
// import com.typesafe.scalalogging.Logger;
import com.typesafe.scalalogging.LazyLogging;


class KineticaPartitionReader (
    val conf: LoaderParams,
    val tableSchema: StructType,
    val pushedCatalystFilters: Array[Filter],
    val requiredSchema: StructType)
    extends InputPartition[InternalRow]
    with InputPartitionReader[InternalRow]
    with LazyLogging {

    // If we are pruning the table columns, then we need to know which
    // columns we're keeping.
    var requiredColumns = new Array[String]( 0 );
    var schemaToUse: StructType = null;
    if ( requiredSchema != null ) {
        // Only fetch the user requested columns
        schemaToUse = requiredSchema;
        requiredColumns = requiredSchema.fieldNames;
    } else {
        // We're not pruning columns from the table; so use the table's
        // entire schema (i.e., all the columns)
        schemaToUse = tableSchema;
        requiredColumns = tableSchema.fieldNames;
    }

    val numPartitions = conf.getNumPartitions;
    val url   = if (conf.getJdbcURL   != null ) conf.getJdbcURL   else sys.error("Option 'database.jdbc_url' not specified");
    val table = if (conf.getTablename != null ) conf.getTablename else sys.error("Option 'table.name' not specified");

    val gpudbConn = conf.getGpudb;
    val tableName = conf.getTablename;

    // Get the table size
    val tableSize = KineticaEgressUtilsNativeClient.getKineticaTableSize( gpudbConn, tableName );
    logger.debug("KineticaInputPartitionReader: got table size {}", tableSize);

    // Read the table's rows in batches (default is 10k)
    val batchSize = conf.getEgressBatchSize;

    // Offset and limit for getting data out
    val offset = conf.getEgressOffset;
    var limit  = conf.getEgressLimit;
    logger.debug( s"KineticaInputPartitionReader constructor: offset $offset limit $limit" );
    if ( limit == null ) {
        // Use the table size as the limit
        limit = tableSize;
    }

    logger.debug(s"tableSchema ${tableSchema.toString}");
    if ( requiredSchema != null ) {
        logger.debug(s"requiredSchema ${requiredSchema.toString}");
    } else {
        logger.debug(s"requiredSchema is NULL");
    }
    logger.debug(s"schemaToUse ${schemaToUse.toString}");

    // Retrieve the records
    val myRows: Iterator[InternalRow] = {

        if ( pushedCatalystFilters.isEmpty ) {
            logger.debug("KineticaInputPartitionReader: No filters given; use the native client API");
            // Use the fastest path via the native Java API to get the records
            // since no filters are used
            KineticaEgressUtilsNativeClient.getInternalRowsFromKinetica( gpudbConn, tableName,
                                                                         requiredColumns, schemaToUse,
                                                                         offset, limit, batchSize );
        } else {
            logger.debug("KineticaInputPartitionReader: Filters ARE given; use the JDBC connector");
            // Use the JDBC connector to get the records
            val queryStr = buildTableQuery( table, requiredColumns, pushedCatalystFilters,
                                            offset, limit );

            val conn = com.kinetica.spark.egressutil.KineticaJdbcUtils.getConnector(url, conf)();
            val stmt = conn.prepareStatement( queryStr );
            val rs = stmt.executeQuery;

            val internalRows = KineticaEgressUtilsJdbc.resultSetToSparkInternalRows( rs, schemaToUse );
            val encoder = org.apache.spark.sql.catalyst.encoders.RowEncoder.apply( schemaToUse ).resolveAndBind();
            internalRows;
        }
    }

    // Override the method in the InputPartition trait that actually instantiates
    // one of the KineticaPartitionReader objects
    override def createPartitionReader(): InputPartitionReader[InternalRow] = {
        new KineticaPartitionReader( conf, tableSchema, pushedCatalystFilters, requiredSchema );
    }

    override def next(): Boolean = {
        myRows.hasNext
    }

    override def get(): InternalRow = {
        myRows.next()
    }

    override def close(): Unit = {
    }


    private def buildTableQuery(
        table: String,
        columns: Array[String],
        pushedCatalystFilters: Array[Filter],
        offset: Long,
        limit: Long
        ): String = {

        val baseQuery = {

            val colStrBuilder = new StringBuilder();

            if (columns.length > 0) {
                colStrBuilder.append( columns(0) );
                columns.drop(1).foreach( col => colStrBuilder.append(",").append(col) );
            } else {
                // If no columns are explicitly specified, then we would get everything,
                // hence the 1
                colStrBuilder.append("1")
            }

            // Generate a where clause based on all the filters, along with the offset & limit
            val whereClause = KineticaFilters.getWhereClause( pushedCatalystFilters,
                                                              offset,
                                                              limit )

            // Need to quote the schema name and the table name separately
            var quotedTableName = KineticaJdbcUtils.quoteTableName( table );

            // Need to quote the table name, but quotes don't work with string
            // interpolation in scala; the following is correct, though ugly
            s"""/* KI_HINT_MAX_ROWS_TO_FETCH($limit) */ SELECT $colStrBuilder FROM "$quotedTableName" $whereClause"""
        }
        logger.info("Query for retrieving records: " + baseQuery)
        baseQuery.toString()
    }   // end buildTableQuery


}  // end class KineticaInputPartitionReader
