package com.kinetica.spark.datasourcev1

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.RelationProvider

import com.kinetica.spark.util.Constants

/*
 * This is the default datasource class which creates and returns the KineticaRelation class doing the actual save/fetch.
 */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
        try {
            new KineticaRelation(parameters, sqlContext.sparkSession)
        } catch {
            case re: RuntimeException => throw re
            case e: Exception => throw new RuntimeException(e)
        }
    }

    override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], df: DataFrame): BaseRelation = {
        try {
            // TODO: What to do with the saveMode?
            val kineticaRelation: KineticaRelation = new KineticaRelation(parameters, Some(df), sqlContext.sparkSession)
            kineticaRelation.insert(df, true)
            kineticaRelation
        } catch {
            case re: RuntimeException => throw re
            case e: Exception => throw new RuntimeException(e)
        }
    }

    override def shortName(): String = Constants.KINETICA_FORMAT
}
