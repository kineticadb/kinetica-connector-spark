# File: ingest_csv.py
# Purpose: Example ingest
###############################################################################

import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName('ingest_csv')\
        .getOrCreate()

    tableDF = spark.read.format('csv') \
        .options(header='true', inferSchema='false') \
        .load('../data/flights.csv')

    tableName = "flights"

    print('Writing records to {}: {}'.format(tableName, tableDF.count()))

    tableDF.write.format('com.kinetica.spark') \
        .option('kinetica-url', 'http://gpudb:9191') \
        .option('kinetica-jdbcurl', 'jdbc:simba://gpudb:9292;ParentSet=test') \
        .option('kinetica-desttablename', tableName) \
        .option('kinetica-replicatedtable' ,'false') \
        .option('kinetica-maptoschema', 'false') \
        .option('kinetica-createtable', 'true') \
        .save()

    print('All done!')
