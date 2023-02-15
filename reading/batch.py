from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import *
from time import sleep

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("StorageToHDFS") \
    .getOrCreate()

batch_time = 10
path = "../datas"
while (True):

    df = spark.readStream.option(header='True', inferSchema='True').csv(path)
# Écrire les données dans HDFS
    df.write.csv("hdfs://localhost:9000/cluster/")
    df.show(10)
    sleep(batch_time)
spark.stop()