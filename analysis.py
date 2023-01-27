from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, IntegerType
from pyspark.sql.functions import udf

udf_month = udf(lambda date: date[:-3], StringType())

def get_nomads(df, start_date, end_date, threshold=3):
    df_sorties = df.filter(df["Déplacements"] == "Sortie")
    sdate = start_date[:-3]
    edate = end_date[:-3]
    df_with_month = df_sorties.withColumn("month", udf_month("Dates"))
    df_new = df_with_month.filter((df_with_month["month"] >= sdate) & (df_with_month["month"] <= edate))
    df_counts = df_new.groupBy("Noms", "month").count()
    df_counts = df_counts.drop("Dates", "Déplacements", "Documents",
                         "Numéro_du_document", "Frontières", "Destinations")
    df_counts = df_counts.filter(df_counts["count"] >= threshold)
    return df_counts

def get_scammers(df, start_date, end_date, threshold=3):
    sdate = start_date[:-3]
    edate = end_date[:-3]
    df_with_month = df.withColumn("month", udf_month("date"))
    df_new = df_with_month.filter((df_with_month["month"] >= sdate) & (df_with_month["month"] <= edate))
    df_counts = df_new.groupBy("relative_to", "month").count()
    df_counts = df_counts.drop("sender", "receiver", "operator",
                         "type", "amount")
    df_counts = df_counts.filter(df_counts["count"] >= threshold)
    return df_counts
