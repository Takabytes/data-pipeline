from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import *
from time import sleep
import os
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, IntegerType

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialiser la session Spark
spark = SparkSession.builder\
	.master("local").appName("pyspark_stream_setup").getOrCreate()

path = "../datas/"
hdfs_oni = "hdfs://localhost:9000/cluster/oni/"
hdfs_moves = "hdfs://localhost:9000/cluster/moves/"
# hdfs_finances = "hdfs://localhost:9000/cluster/finances"

customSchema = StructType() \
		 .add("ID", IntegerType(), True) \
		 .add("Name", StringType(), True) \
		 .add("Sex", StringType(), True) \
		 .add("Address", StringType(), True) \
		 .add("E-mail", StringType(), True) \
         .add("Date-of-birth", DateType(), True) \
         .add("Phone-number", StringType(), True) \
         .add("Job", StringType(), True)

moveSchema = StructType() \
		 .add("Dates", DateType(), True) \
		 .add("Noms", StringType(), True) \
		 .add("Déplacements", StringType(), True) \
		 .add("Documents", StringType(), True) \
		 .add("Numéro_du_document", StringType(), True) \
         .add("Frontières", StringType(), True) \
         .add("Destinations", StringType(), True)


# Create a DataFrame from the input text file

df1 = spark.readStream.schema(customSchema).csv("../datas/oni/")
df2 = spark.readStream.schema(moveSchema).csv("../datas/moves/")
# Écrire les données dans HDFS
query1 = df1.writeStream.format("csv").option("path", hdfs_oni).option("checkpointLocation", "/tmp/temp1/checkpoint").start()
# query1.awaitTermination()
query2 = df2.writeStream.format("csv").option("path", hdfs_moves).option("checkpointLocation", "/tmp/temp2/checkpoint").start()
query1.awaitTermination()
query2.awaitTermination()
# df.writeStream\
# .format("console").outputMode("append").start()
# df.writeStream.text("hdfs://localhost:9000/cluster/")
df1.isStreaming
df2.isStreaming

spark.stop()