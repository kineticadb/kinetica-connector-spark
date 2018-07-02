import sys
from pyspark.sql import SparkSession

"""
A simple example demonstrating Kinetica ingest.
./spark-submit --master "spark://172.31.70.10:7077"
  --jars <targetdir>/kinetica-spark-6.2.1-jar-with-dependencies.jar
  <somedir>/python/kineticaingest.py
  <data_file_path>
  <db_host> [<db_user> <db_pass>]
"""


def basic_ingest_example():

    host = db_host
    username = db_user
    password = db_pass
    url = "http://%s:9191" % host
    options = {
        "database.url" : url,
        "database.jdbc_url" : "jdbc:simba://%s:9292;URL=%s;ParentSet=MASTER" % (host, url),
        "database.username" : username,
        "database.password" : password,
        "table.name" : "airline",
        "table.is_replicated" : "false",
        "table.map_columns_by_name" : "false",
        "table.create" : "true",
        "table.truncate" : "true"
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

    if (len(sys.argv) < 3):
        print ("Usage: %s <data_file_path> <db_host> [<db_user> <db_pass>]" % sys.argv[0])
        quit()

    data_file = sys.argv[1]
    db_host = sys.argv[2]
    db_user = sys.argv[3] if len(sys.argv) > 3 else ""
    db_pass = sys.argv[4] if len(sys.argv) > 4 else ""

    basic_ingest_example()
