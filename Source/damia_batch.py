from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import *

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("StorageToHDFS") \
    .master("local") \
    .getOrCreate()

batch_time = 10
id_path = "../data/*identities.csv"
moves_path = "../data/*moves.csv"
momo_path = "../data/momo*"
# judicial_path = "/Users/pepita/msc1/parttime/generator/algos/data-pipeline/data/*infractions.json"

# while (True):
id_df = spark.read.options(header='True', inferSchema='True').csv(id_path)
moves_df = spark.read.options(header='True', inferSchema='True').csv(moves_path)
momo_df = spark.read.options(header='True', inferSchema='True').csv(momo_path)
# judicial_df = spark.read.options(header='True', inferSchema='True').json(judicial_path)

# Écrire les données dans HDFS
id_df.write.mode('overwrite').parquet("hdfs://localhost:9000/cluster/identity/identification/")
moves_df.write.mode('overwrite').parquet("hdfs://localhost:9000/cluster/moves/transports/")
momo_df.write.mode('overwrite').parquet("hdfs://localhost:9000/cluster/finances/mobile_money/")
# judicial_df.write.mode('overwrite').json("hdfs://localhost:9000/cluster/judicial/infractions/")

id_df.show(10)

spark.stop()