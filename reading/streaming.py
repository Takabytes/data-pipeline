from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import *
from time import sleep

def stream(spark, fileUrl, schema, outputDir, checkpointDir):
    df = (spark
          .readStream
          .format('csv')
          .option('header', True)
          .option('path', fileUrl)
          .schema(schema)
          .load())
    query = (df
             .writeStream
             .format('parquet')
             .trigger(processingTime='10 second')
             .option('path', outputDir)
             .option('checkpointLocation', checkpointDir)
             .start())
    query.awaitTermination()

#Test
spark = (SparkSession
         .builder
         .appName('StorageToHDFS')
         .getOrCreate())

moveSchema = StructType([
    StructField('Nom', StringType(), True),
    StructField('Type', StringType(), True),
    StructField('Document_utilisé', StringType(), True),
    StructField('Numéro_du_document', IntegerType(), True),
    StructField('Provenance', StringType(), True),
    StructField('Destination', StringType(), True),
    StructField('Date', StringType(), True)
])

hdfs_moves = 'hdfs://localhost:9000/cluster/moves/'

stream(spark, '../data/*moves.csv', moveSchema, hdfs_moves, '/tmp/temp2/checkpoint/')
