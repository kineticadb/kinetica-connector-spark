import sys
from pyspark.sql import SparkSession

"""
A simple example demonstrating Kinetica ingest.
./spark-submit --master "spark://172.31.70.10:7077"  --jars <targetdir>/kinetica-spark-6.2.1-jar-with-dependencies.jar <somedir>/python/kineticaingest.py [data_file_path] [table_name] [db_host]
"""


def basic_ingest_example():

    host = db_host
    URL = "http://%s:9191" % host
    options = {
        "database.url" : URL,
        "database.jdbc_url" : "jdbc:simba://%s:9292;URL=%s;ParentSet=MASTER" % (host, URL),
        "table.name" : "airline",
        "table.is_replicated" : "false",
        "table.map_columns_by_name" : "false",
        "table.create" : "true"
    }

    spark = SparkSession \
        .builder \
        .appName("Python Spark Kinetica data source ingest example") \
        .getOrCreate()

    df = spark.read.load(
        data_file,
        format='com.databricks.spark.csv',
        header='true',
        inferSchema='true',
        delimeter=','
    )
        
    df.write.format("com.kinetica.spark").options(**options).save()

    spark.stop()


if __name__ == "__main__":

    if (len(sys.argv) != 4):
        print ("Usage: %s [data_file_path] [table_name] [db_host]" % sys.argv[0])
        quit()

    data_file = sys.argv[1]
    table_name = sys.argv[2]
    db_host = sys.argv[3]

    basic_ingest_example()
