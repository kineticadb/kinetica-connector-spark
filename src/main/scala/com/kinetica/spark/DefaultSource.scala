package com.kinetica.spark

import com.kinetica.spark.util.Constants

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.RelationProvider


/*
 * This default datasource class implements the DataSource v1 interface.  Please
 * com.kinetica.spark.datasourcev1.DefaultSource for futher implementation
 * details.
 */
class DefaultSource extends com.kinetica.spark.datasourcev1.DefaultSource {}
