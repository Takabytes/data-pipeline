from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import *
from functions import *

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("StorageToHDFS") \
    .master("local") \
    .getOrCreate()

id_source_path = "hdfs://localhost:9000/cluster/identity/identification/"
id_dest_path = "hdfs://localhost:9000/working/identity/identification/"

moves_source_path = "hdfs://localhost:9000/cluster/moves/transports/"
moves_dest_path = "hdfs://localhost:9000/working/moves/transports/"

momo_source_path = "hdfs://localhost:9000/cluster/finances/mobile_money/"
momo_dest_path = "hdfs://localhost:9000/working/finances/mobile_money/"

# judicial_source_path = "hdfs://localhost:9000/cluster/judicial/infractions/"
# judicial_dest_path = "hdfs://localhost:9000/working/judicial/infractions/"

move_data(spark, id_source_path, id_dest_path)
move_data(spark, moves_source_path, moves_dest_path)
move_data(spark, momo_source_path, momo_dest_path)

# df = spark.read.options(header='True', inferSchema='True').json(judicial_source_path)
# df.write.mode('overwrite').json(judicial_dest_path)
# move_data(spark, judicial_source_path, judicial_dest_path)

spark.stop()

