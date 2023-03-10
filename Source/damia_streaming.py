from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import *
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialiser la session Spark
spark = SparkSession.builder\
	.master("local").appName("pyspark_stream_setup").getOrCreate()

hdfs_oni = "hdfs://localhost:9000/cluster/identity/identification/"
hdfs_moves = "hdfs://localhost:9000/cluster/moves/transports/"
hdfs_momo = "hdfs://localhost:9000/cluster/finances/mobile_money/"
hdfs_judicial = "hdfs://localhost:9000/cluster/judicial/infractions/"

customSchema = StructType() \
		 .add("Nom", StringType(), True) \
		 .add("Date_de_naissance", DateType(), True) \
		 .add("Sexe", StringType(), True) \
		 .add("Adresse", StringType(), True) \
		 .add("E-mail", StringType(), True) \
         .add("Numéro_de_téléphone", StringType(), True) \
         .add("Job", StringType(), True)

moveSchema = StructType() \
		 .add('Nom', StringType(), True) \
		 .add('Type', StringType(), True) \
		 .add('Document_utilisé', StringType(), True)\
		 .add('Numéro_du_document', IntegerType(), True) \
		 .add('Provenance', StringType(), True) \
         .add('Destination', StringType(), True) \
         .add('Date', DateType(), True)

momoSchema = StructType() \
		 .add('Type', StringType(), True) \
		 .add('Montant', IntegerType(), True) \
		 .add('Nom_src', StringType(), True)\
		 .add('Ancien_solde_src',  IntegerType(), True) \
		 .add('Nom_dest', StringType(), True) \
         .add('Ancien_solde_dest', IntegerType(), True) \
         .add('Nouveau_solde_dest', IntegerType(), True) \
		 .add('Date', DateType(), True)

judicialSchema = StructType() \
		 .add('Nom', StringType(), True) \
		 .add('Age', IntegerType(), True) \
		 .add('Type_d_infraction', StringType(), True)\
		 .add('Infraction', StringType(), True) \
		 .add('Date_d_infraction', DateType(), True)

# Create a DataFrame from the input text file

df1 = spark.readStream.schema(customSchema).csv("../data/*identities.csv")
df2 = spark.readStream.schema(moveSchema).csv("../data/*moves.csv")
df3 = spark.readStream.schema(momoSchema).csv("../data/momo*")
df4 = spark.readStream.schema(judicialSchema).json("../data/*infractions.json")

# Écrire les données dans HDFS
query1 = df1.writeStream.format("csv").option("path", hdfs_oni).option("checkpointLocation", "/tmp/temp1/checkpoint").start()
query2 = df2.writeStream.format("csv").option("path", hdfs_moves).option("checkpointLocation", "/tmp/temp2/checkpoint").start()
query3 = df3.writeStream.format("csv").option("path", hdfs_momo).option("checkpointLocation", "/tmp/temp3/checkpoint").start()
query4 = df4.writeStream.format("csv").option("path", hdfs_judicial).option("checkpointLocation", "/tmp/temp4/checkpoint").start()

query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
query4.awaitTermination()

df1.isStreaming
df2.isStreaming
df3.isStreaming
df4.isStreaming

spark.stop()