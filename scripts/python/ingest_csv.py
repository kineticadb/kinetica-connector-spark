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
        .option('database.url', 'http://gpudb:9191') \
        .option('database.jdbc_url', 'jdbc:kinetica://gpudb:9191;ParentSet=test') \
        .option('table.name', tableName) \
        .option('table.is_replicated' ,'false') \
        .option('table.map_columns_by_name', 'false') \
        .option('table.create', 'true') \
        .save()

    print('All done!')
