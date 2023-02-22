from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import *
from time import sleep

def batch(spark, fileUrl, outputDir, batchTime=10):
    while True:
        df = (spark
              .read
              .option('header', 'true')
              .option('inferSchema', 'true')
              .csv(fileUrl))
        df.write.mode('overwrite').parquet(outputDir)


        df.show(10)


        sleep(batchTime)


#Test
spark = (SparkSession
         .builder
         .appName('StorageToHDFS')
         .getOrCreate())

hdfs_oni = 'hdfs://localhost:9000/cluster/oni/'
batch(spark, '../data/*identities.csv', hdfs_oni)

