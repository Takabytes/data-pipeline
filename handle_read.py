from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, IntegerType

#Build spark session
spark = SparkSession.builder.appName("Data Pipeline").getOrCreate()

#Set Logging level to WARN
spark.sparkContext.setLogLevel("WARN")

def spark_reader(path, schema):
    df = spark.read.option('header', True).csv(path, schema=schema).cache()
    return df

# Load the two dataframes
def get_schema_momo():
    schema_momo = StructType([
        StructField("relative_to", StringType()),
        StructField("sender", StringType()),
        StructField("receiver", StringType()),
        StructField("operator", StringType()),
        StructField("type", StringType()),
        StructField("date", StringType()),
        StructField("amount", IntegerType())
    ])
    return schema_momo

def get_schema_move():
    schema_move = StructType([
        StructField("Dates", StringType()),
        StructField("Noms", StringType()),
        StructField("Déplacements", StringType()),
        StructField("Documents", StringType()),
        StructField("Numéro_du_document", IntegerType()),
        StructField("Frontières", StringType()),
        StructField("Destinations", StringType())
    ])
    return schema_move
