package com.kinetica.spark.util.table;

import com.google.common.base.CharMatcher;
import org.apache.spark.sql._;
import org.apache.spark.sql.types._;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import scala.beans.{ BeanProperty, BooleanBeanProperty };
import com.typesafe.scalalogging.LazyLogging;

import com.gpudb.Type;
import com.gpudb.protocol.HasTableResponse;


import com.kinetica.spark.LoaderParams;
import com.kinetica.spark.util.JDBCConnectionUtils;
import com.kinetica.spark.util.KineticaSparkDFManager;

import scala.collection.JavaConversions._;



object SparkKineticaTableUtil extends LazyLogging {

    var shardkeys: List[String] = new ArrayList[String]()

    var primarykeys: List[String] = new ArrayList[String]()

    var textsearchfields: List[String] = new ArrayList[String]()

    var storeonlyfields: List[String] = new ArrayList[String]()

    var ipv4fields: List[String] = new ArrayList[String]()

    var diskoptimizedfields: List[String] = new ArrayList[String]()

    var wktfields: List[String] = new ArrayList[String]()

    var notnullfields: List[String] = new ArrayList[String]()

    var dictencodingfields: List[String] = new ArrayList[String]()

    var stringfields: List[String] = new ArrayList[String]()

    var allnotnull: Boolean = false

    var allStringDict: Boolean = false

    var snappyfields: List[String] = new ArrayList[String]()

    var lz4fields: List[String] = new ArrayList[String]()

    var lz4hcfields: List[String] = new ArrayList[String]()

    @BeanProperty
    var alterStatements: List[String] = new ArrayList[String]()

    @BeanProperty
    var charColumnLengths: scala.collection.mutable.Map[String, Integer] = scala.collection.mutable.Map();

    def init(): Unit = {
        shardkeys = new ArrayList[String]()
        primarykeys = new ArrayList[String]()
        textsearchfields = new ArrayList[String]()
        storeonlyfields = new ArrayList[String]()
        ipv4fields = new ArrayList[String]()
        diskoptimizedfields = new ArrayList[String]()
        wktfields = new ArrayList[String]()
        notnullfields = new ArrayList[String]()
        dictencodingfields = new ArrayList[String]()
        stringfields = new ArrayList[String]()
        allnotnull = false
        allStringDict = false
        snappyfields = new ArrayList[String]()
        lz4fields = new ArrayList[String]()
        lz4hcfields = new ArrayList[String]()
        alterStatements = new ArrayList[String]()
    }

    def verifyJdbcUrl(lp: LoaderParams): Unit = {
        try if (lp.getJdbcURL.trim().isEmpty)
            throw new RuntimeException("Missing JDBC URL or invalid")
        catch {
            case e: Exception => {
                logger.error("JDBC url missing")
                throw new RuntimeException("Missing JDBC URL or invalid")
            }
        }
    }


    /**
     * Check if the table exists in Kinetica.
     *
     * @param tableName  The name of the table
     * @param properties Loader parameters
     *
     * @return true if the table exists, false otherwise
     */
    def hasTable( tableName: String, properties: LoaderParams ): Boolean = {
        val db = properties.getGpudb()
        try {
            val result = db.hasTable( tableName, null );
            return result.getTableExists();
        } catch {
            // Could not retrieve any table, probably because the table doesn't exist
            case re: RuntimeException => throw re
            case e: Exception => throw new RuntimeException( e )
        }
    }

    /**
     * Creates Kinetica table from spark dataframe or a schema type
     * @param ds Option[DataFrame]
     * @param schema Option[StructType]
     * @param lp LoaderParams
     * @throws KineticaException
     */
    def createTable(ds: Option[DataFrame], schema: Option[StructType], lp: LoaderParams): Unit = {
        logger.info("Creating table from schema")
        if ( !lp.isDryRun() ) {
            verifyJdbcUrl(lp)
        }
        buildCreateTableDDL(ds, schema, lp)
        //execute create table
        logger.info("Create table DDL:  {}", KineticaDDLBuilder.getCreateTableDDL)

        if( !lp.isDryRun() && !lp.hasTable() ) {
            JDBCConnectionUtils.Init(lp)
            logger.info("Creating table " + lp.getTablename);
            JDBCConnectionUtils.executeSQL(KineticaDDLBuilder.getCreateTableDDL)
            /*Execute alter statements - SIMPLY SAY NO
        		val iterator: Iterator[String] =
          		SparkKineticaTableUtil.getCompressDDLs.iterator()
        		while (iterator.hasNext) JDBCConnectionUtils.executeSQL(iterator.next())
        	*/
            init()
        }
    }

    def truncateTable(ds: Option[DataFrame], lp: LoaderParams): Unit = {
        logger.debug("truncateTable")
        verifyJdbcUrl(lp)
        JDBCConnectionUtils.Init(lp)
        //execute truncate table
        JDBCConnectionUtils.executeSQL(s"TRUNCATE TABLE ${lp.tablename}")
        JDBCConnectionUtils.close()
    }

    def tableExists(lp: LoaderParams): Boolean = {
        logger.debug("tableExists")
        verifyJdbcUrl(lp)
        JDBCConnectionUtils.Init(lp)
        val te = JDBCConnectionUtils.tableExists(lp.tablename)
        JDBCConnectionUtils.close()
        te
    }

    /**
     * Kinetica table must already exist to use this method
     * <p>
     *     This method will evaluate spark dataframe columns against Kinetica table columns
     *     and determine which do not.  For those columns which exist in spark dataframe
     *     and do not exist in Kinetica, the method will create those columns in the Kinetica table
     * </p>
     * @param ds Spark Dataframe/Dataset
     * @param lp LoaderParams
     * @throws KineticaException
     */
    def AlterTable(ds: DataFrame, lp: LoaderParams): Unit = {
        logger.debug("Mapping DataFrame columns to Kinetica")
        try if (lp.getJdbcURL.trim().isEmpty)
            throw new RuntimeException("Missing JDBC URL or invalid")
        catch {
            case e: Exception => {
                logger.error("JDBC url missing")
                throw new RuntimeException("Missing JDBC URL or invalid")
            }

        }
        buildAlterDDL(Option.apply(ds), lp)
        JDBCConnectionUtils.Init(lp)
        var iterator: Iterator[String] = null
        //execute alter table statements
        iterator = getAlterStatements.iterator()
        while (iterator.hasNext) {
            val alterStatement: String = iterator.next()
            logger.info("Executing alter Statement: " + alterStatement)
            JDBCConnectionUtils.executeSQL(alterStatement)
        }
        /*Execute alter compress statements JAZZ
    		iterator = SparkKineticaTableUtil.getCompressDDLs.iterator()
    		while (iterator.hasNext) JDBCConnectionUtils.executeSQL(iterator.next())
	    */
        init()
    }

    /**
     * Builds Kinetica DDL from either a DataFrame or a schema
     * @param ds Spark Dataframe/Dataset
     * @param schema StructType  Schema for the table
     * @param lp LoaderParams object
     */
    def buildCreateTableDDL(ds: Option[DataFrame], schema: Option[StructType], lp: LoaderParams): Unit = {
        iniAlterStatements()
        KineticaDDLBuilder.init(lp)
        val alterDDL: Boolean = false

        // Get the schema
        var sField: scala.collection.Iterator[StructField] = null
        ds match {
            case Some(ds) => sField = ds.schema.iterator;
            case None => {
                // Since no dataframe is given, a StructType better be given!
                schema match {
                    case Some(schema) => sField = schema.iterator;
                    case None => throw new RuntimeException("Need either a dataFrame or a StructType; neither given")
;
                }
            }
        }

        charColumnLengths.clear();

        while (sField.hasNext) {
            val sf2: StructField = sField.next()
            val dt: DataType = sf2.dataType

            //System.out.println(" sf2 ####################### " + sf2.name)
            //System.out.println(" dt ####################### " + dt)

            // We need to quote the column names because the 7.0
            // JDBC connector cannot handle column names
            // beginning with numbers.
            val colName = ColumnProcessor.quoteColumnName( sf2.name )

            if (dt.isInstanceOf[DecimalType]) {
                logger.debug("Found DecimalType")
                ColumnProcessor.processDecimal(
                    dt,
                    colName,
                    sf2.nullable,
                    alterDDL)
            }
            else if (dt.isInstanceOf[NumericType]) {
                logger.debug("Found NumericType")
                ColumnProcessor.processNumeric(
                    dt,
                    colName,
                    sf2.nullable,
                    alterDDL)
            } else if (dt.isInstanceOf[StringType]) {
                logger.debug("Found StringType")
                val dryRun = lp.isDryRun()
                ColumnProcessor.processString(
                    ds,
                    colName,
                    sf2.nullable,
                    alterDDL,
                    false,
                    dryRun)
            } else if (dt.isInstanceOf[TimestampType]) {
                logger.debug("Found TimestampType")
                ColumnProcessor.processTS(ds, colName, sf2.nullable, alterDDL)
            } else if (dt.isInstanceOf[DateType]) {
                logger.debug("Found DateType")
                ColumnProcessor.processDate(ds, colName, sf2.nullable, alterDDL)
            } else if (dt.isInstanceOf[BooleanType]) {
                logger.debug("Found BooleanType")
                ColumnProcessor.processBoolean(ds, colName, sf2.nullable, alterDDL)
            } else if (dt.isInstanceOf[BinaryType]) {
                logger.debug("Found BinaryType")
                ColumnProcessor.processByteArray(ds, colName, sf2.nullable, alterDDL)
            } else {
                logger.debug("Found complex type perhaps")
                val dryRun = lp.isDryRun()
                ColumnProcessor.processString(
                    ds,
                    colName,
                    sf2.nullable,
                    alterDDL,
                    false,
                    dryRun,
                    // lastly unrestricted string
                    true)
            }
        }
        logger.info(" @@@@@@@@@@@@ String column name and lengths found " + getCharColumnLengths())
        KineticaDDLBuilder.closeDDL()
    }

    def buildAlterDDL(ds: Option[DataFrame], lp: LoaderParams): Unit = {
        AlterTableAddColumnDDL.init(lp)
        AlterTableModifyColumnDDL.init(lp)
        iniAlterStatements()
        val mytype: Type = lp.getTableType();
        val alterDDL: Boolean = true

        // Get the field iterator
        var sField: scala.collection.Iterator[StructField] = null
        ds match {
            case Some(ds) => sField = ds.schema.iterator;
            case None => throw new KineticaException("No data frame given!");
        }

        while (sField.hasNext) {
            var columnFound: Boolean = false
            val sf2: StructField = sField.next()
            val dt: DataType = sf2.dataType

            var myColumns = mytype.getColumns
            for (column <- myColumns) {
                logger.debug(
                    "Eval if " + sf2.name + "equals Kinetica column: " +
                        column.getName)
                if ( ColumnProcessor.areColumnNamesEqual( sf2.name, column.getName ) ) {
                    columnFound = true
                    logger.debug(column.getName + " column found")
                }
            }

            // We need to quote the column names because the 7.0
            // JDBC connector cannot handle column names
            // beginning with numbers.
            val colName = ColumnProcessor.quoteColumnName( sf2.name )

            // Column not found, add column to table
            if (!columnFound) {

                logger.debug( s"$colName column not found, processing" )
                AlterTableAddColumnDDL.init(lp)
                if (dt.isInstanceOf[NumericType]) {
                    logger.debug("Found NumericType")
                    ColumnProcessor.processNumeric(
                        dt,
                        colName,
                        sf2.nullable,
                        alterDDL)
                    closeAddAlter(colName, lp.getTablename)
                } else if (dt.isInstanceOf[StringType]) {
                    logger.debug("Found StringType")
                    ColumnProcessor.processString(
                        ds,
                        colName,
                        sf2.nullable,
                        alterDDL,
                        columnFound,
                        false)
                    closeAddAlter(colName, lp.getTablename)
                } else if (dt.isInstanceOf[TimestampType]) {
                    logger.debug("Found TimestampType")
                    ColumnProcessor.processTS(ds, colName, sf2.nullable, alterDDL)
                    closeAddAlter(colName, lp.getTablename)
                } else if (dt.isInstanceOf[DateType]) {
                    logger.debug("Found DateType")
                    ColumnProcessor.processDate(ds, colName, sf2.nullable, alterDDL)
                    closeAddAlter(colName, lp.getTablename)
                } else if (dt.isInstanceOf[BooleanType]) {
                    logger.debug("Found BooleanType")
                    ColumnProcessor.processBoolean(
                        ds,
                        colName,
                        sf2.nullable,
                        alterDDL)
                    closeAddAlter(colName, lp.getTablename)
                }
            } else {
                // Column already exists; we're just modifying it
                AlterTableModifyColumnDDL.init(lp)
                if (dt.isInstanceOf[StringType]) {
                    logger.debug("Found StringType")
                    ColumnProcessor.processString(
                        ds,
                        colName,
                        sf2.nullable,
                        alterDDL,
                        columnFound,
                        false)
                    closeModAlter(colName, lp.getTablename)
                }
            }
        }
    }

    /**
     * Close alter statement and add to alter list
     * @param columnName column name
     * @param tableName table to be altered
     */
    private def closeAddAlter(columnName: String, tableName: String): Unit = {
        logger.debug("closing")
        // JAZZ BuildCompressDDL.buildDDL(columnName, tableName)
        setAlterStatements(AlterTableAddColumnDDL.getAlterTableDDL)
    }

    /**
     * close alter statement and add to alter list
     * @param columnName column name
     * @param tableName table to be altered
     */
    private def closeModAlter(columnName: String, tableName: String): Unit = {
        logger.debug("closing")
        if (AlterTableModifyColumnDDL.columnAlterDetected()) {
            setAlterStatements(AlterTableModifyColumnDDL.getAlterTableDDL)
        }
    }

    /**
     * Return kinetica DDL from dataframe
     * @param ds Option[DataFrame] Optional data frame
     * @param schema Option[StructType]  The schema for the table
     * @param lp LoaderParams
     * @return Generate DDL
     */
    def getCreateTableDDL(ds: Option[DataFrame], schema: Option[StructType], lp: LoaderParams): String = {
        buildCreateTableDDL(ds, schema, lp)
        KineticaDDLBuilder.getCreateTableDDL
    }


    /**
     * Return kinetica Alter DDL from dataframe
     * @param ds Dataframe
     * @param lp LoaderParams
     * @return Generate DDL
     */
    def getAlterTableDDL(ds: DataFrame, lp: LoaderParams): List[String] = {
        buildAlterDDL(Option.apply(ds), lp)
        getAlterStatements
    }

    /**
     * <pre>Set shard key for table creation
     * May be called multiple times to set multiple shard keys
     * </pre>
     *
     * @param shardKey Shardkey
     */
    def setShardKey(shardKey: String): Unit = {
        if ( !shardkeys.contains(shardKey) ) {
            shardkeys.add(shardKey)
        }
    }

    /**
     * <pre>Set shard keys for table creation
     * </pre>
     * @param shardKeys List of shard keys
     */
    def setShardKeys(shardKeys: List[String]): Unit = {
        for (shardkey <- shardKeys) {
            if (!shardkeys.contains(shardkey))
                shardkeys.add(shardkey)
        }
    }

    /**
     * Return shard keys
     * @return shard keys
     */
    def getShardkeys(): List[String] = shardkeys

    /**
     * Returns primary keys
     * @return list of primary keys
     */
    def getPrimarykeys(): List[String] = primarykeys

    /**
     * Set primary keys for table creation
     * @param pks list of primary keys
     */
    def setPrimarykeys(pks: List[String]): Unit = {
        for (key <- pks) {
            if (!primarykeys.contains(key))
                primarykeys.add(key)
        }
    }

    /**
     * <pre>
     * Set primary key.  May be called multiple times to set multiple columns to primary key
     * </pre>
     * @param primarykey column
     */
    def setPrimarykey(primarykey: String): Unit = {
        if (!primarykeys.contains(primarykey)) {
            primarykeys.add(primarykey)
        }
    }

    /**
     * Get fields set to text_search
     * @return list of columns
     */
    def getTextsearchfields(): List[String] = textsearchfields

    /**
     * Set text search columns
     * @param tsFields list of columns
     */
    def setTextsearchfields(tsFields: List[String]): Unit = {
        for (key <- tsFields){
            if (!textsearchfields.contains(key))
                textsearchfields.add(key)
        }

    }

    /**
     * <pre>
     * Set column to text search
     * May be called multiple times to set multiple to text search
     * </pre>
     * @param textsearchfield text search column
     */
    def setTextsearchfield(textsearchfield: String): Unit = {
        if (!textsearchfields.contains(textsearchfield)) {
            textsearchfields.add(textsearchfield)
        }
    }

    /**
     * Returns store only columns
     * @return list of store only columns
     */
    def getStoreonlyfields(): List[String] = storeonlyfields

    /**
     * <pre>
     *  Set columns to store only
     * </pre>
     * @param sofields list of store only columns
     */
    def setStoreonlyfields(sofields: List[String]): Unit = {
        for (key <- sofields) {
            if (!storeonlyfields.contains(key))
                storeonlyfields.add(key)
        }
    }

    /**
     * <pre>
     * Set column to store only
     * May be called multiple times to set multiple columns to store only
     * </pre>
     * @param storeonlyfield list of store only columns
     */
    def setStoreonlyfield(storeonlyfield: String): Unit = {
        if (!storeonlyfields.contains(storeonlyfield)) {
            storeonlyfields.add(storeonlyfield)
        }
    }

    /**
     * Returns list of IPV4 columns
     * @return list of ipv4 columns
     */
    def getIpv4fields(): List[String] = ipv4fields

    /**
     * Set columns to ivp4 subyet
     * @param ipvfourfields list of columns to set to ipv4 subtype
     */
    def setIpv4fields(ipvfourfields: List[String]): Unit = {
        for (key <- ipvfourfields) {
            if (!ipv4fields.contains(key))
                ipv4fields.add(key)
        }
    }

    /**
     * <pre>
     *     set column to ipv4 subtype
     *     May be called multiple times to set multiple columns to ivp4 subtype
     * </pre>
     * @param ipv4field list of ipv4 columns
     */
    def setIpv4field(ipv4field: String): Unit = {
        if (!ipv4fields.contains(ipv4field)) {
            ipv4fields.add(ipv4field)
        }
    }

    /**
     * Returns list of columns set to disk optimized
     * @return list of columns
     */
    def getDiskoptimizedfields(): List[String] = diskoptimizedfields

    /**
     * Sets columns to subtype disk optimized
     * @param dofields list of columns
     */
    def setDiskoptimizedfields(dofields: List[String]): Unit = {
        for (key <- dofields) {
            if (!diskoptimizedfields.contains(key))
                diskoptimizedfields.add(key)
        }
    }

    /**
     * <pre>
     *     sets column to disk optimized
     *     May be called multiple times to set multiple columns to disk optimized
     * </pre>
     * @param diskoptimizedfield column
     */
    def setDiskoptimizedfield(diskoptimizedfield: String): Unit = {
        if (!diskoptimizedfields.contains(diskoptimizedfield)) {
            diskoptimizedfields.add(diskoptimizedfield)
        }
    }

    /**
     * Returns list of columns set to WKT subtype
     * @return list of columns
     */
    def getWktfields(): List[String] = wktfields

    /**
     * Set columns to subtype WKT
     * @param wkfields list of columns
     */
    def setWktfields(wkfields: List[String]): Unit = {
        for (key <- wkfields) {
            if (!wktfields.contains(key))
                wktfields.add(key)
        }
    }

    /**
     * Set column to subtype WKT
     * @param wktfield column
     */
    def setWktfield(wktfield: String): Unit = {
        if (!wktfields.contains(wktfield)) {
            wktfields.add(wktfield)
        }
    }

    /**
     * Return list of columns set to not null
     * @return list of columns
     */
    def getNotnullfields(): List[String] = notnullfields

    /**
     * Set all columns in list to not null
     * @param nnfields list of columns
     */
    def setNotnullfields(nnfields: List[String]): Unit = {
        for (key <- nnfields) {
            if (!notnullfields.contains(key))
                notnullfields.add(key)
        }
    }

    /**
     * <pre>
     *     Set column to not null
     *     May be called multiple times to set multiple columns to not null
     * </pre>
     * @param notnullfield column
     */
    def setNotnullfield(notnullfield: String): Unit = {
        if (!notnullfields.contains(notnullfield)) {
            notnullfields.add(notnullfield)
        }
    }

    /**
     * Set column to subtype dict
     * @param dictEncodingField column
     */
    def setDictEncodingField(dictEncodingField: String): Unit = {
        if (!dictencodingfields.contains(dictEncodingField)) {
            dictencodingfields.add(dictEncodingField)
        }
    }

    /**
     * Set list of columns to subtype dict
     * @param deFields list of columns
     */
    def setDictEncodingFields(deFields: List[String]): Unit = {
        for (key <- deFields) {
            if (!dictencodingfields.contains(key))
                dictencodingfields.add(key)
        }
    }

    /**
     * Returns list of columns marked for subtype dict
     * @return columns
     */
    def getDictencodingfields(): List[String] =
        dictencodingfields

    /**
     * Set all string columns to dictionary encoding
     */
    def setAllStringsToDict(): Unit = {
        allStringDict = true
    }

    /**
     * Return if all string columns should be set to dict
     * @return true or false
     */
    def isAllStringDict(): Boolean = allStringDict

    /**
     * Returns list of columns marked for subtype dict
     * @return columns
     */
    def getStringfields(): List[String] = stringfields

    /**
     * Set column to be type string.  This will avoid conversion to type charN
     * @param stringfield column
     */
    def setStringfield(stringfield: String): Unit = {
        if (!stringfields.contains(stringfield)) {
            stringfields.add(stringfield)
        }
    }

    /**
     * Set list of columns to type String.  This will avoid conversion to type charN
     * @param sfields columns
     */
    def setStringfields(sfields: List[String]): Unit = {
        for (key <- sfields) {
            if (!stringfields.contains(key))
                stringfields.add(key)
        }
    }

    /**
     * Set all columns to no null
     */
    def setAllNotNull(): Unit = {
        allnotnull = true
    }

    /**
     * Return if all columns are set to not null.
     * This is set via setAllNotNull method
     * @return true or false if all columns are set to not null
     */
    def isAllnotnull(): Boolean = allnotnull

    /**
     * Initialize alter statements list
     */
    def iniAlterStatements(): Unit = {
        alterStatements = new ArrayList[String]()
        // JAZZ compressDDLs = new ArrayList[String]()
    }

    /**
     * Add alter statment to list
     * @param alterStatement alter statement
     */
    def setAlterStatements(alterStatement: String): Unit = {
        if (!alterStatements.contains(alterStatement)) {
            alterStatements.add(alterStatement)
        }
    }

    /**
     *
     * @param compressDDL compress ddl
     */
    def setCompressDDLs(compressDDL: String): Unit = {
        // JAZZ SparkKineticaTableUtil.compressDDLs.add(compressDDL)
    }

}
