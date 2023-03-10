from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import *

def move_data(spark, source_path, dest_path):
    df = spark.read.options(header='True', inferSchema='True').parquet(source_path)
    df.write.mode('overwrite').parquet(dest_path)
    df.show(10)

def transform_data(spark, source_path, dest_path):
    df = spark.read.options(header='True', inferSchema='True').parquet(source_path)
    df.dropDuplicates()
    df.write.mode('overwrite').parquet(dest_path)