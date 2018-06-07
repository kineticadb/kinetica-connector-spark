from pyspark.sql import SparkSession

"""
A simple example demonstrating Kinetica ingest.
./spark-submit --master "spark://172.31.70.10:7077"  --jars <targetdir>/kinetica-spark-6.2.1-jar-with-dependencies.jar <somedir>/python/kineticaingest.py
"""


def basic_ingest_example(spark):

    df = spark.read.load('/home/shouvik/rough/2008.csv',
        format='com.databricks.spark.csv',
        header='true',
        inferSchema='true',
        delimeter='|')
        
    df.write.format("com.kinetica.spark") \
        .option("kinetica-url","http://172.31.70.13:9191") \
        .option("kinetica-desttablename","airline") \
        .option("kinetica-replicatedtable" ,"false") \
        .option("kinetica-maptoschema","false") \
        .option("kinetica-createtable","true") \
        .option("kinetica-jdbcurl","jdbc:simba://172.31.70.13:9292;URL=http://172.31.70.13:9191;ParentSet=MASTER")\
        .save()    

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark Kinetica data source ingest example") \
        .getOrCreate()

    basic_ingest_example(spark)

    spark.stop()
