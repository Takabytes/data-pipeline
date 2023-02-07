from pyspark.sql.functions import substring, when

def get_odds(name, df, groupby, start_date, end_date, threshold=10):
    date_col = [col for col in df.columns if 'date' in col.lower()][0]
    df_with_month = df.withColumn("month", substring(date_col, 1, 7))
    df_new = (df_with_month.filter((df_with_month["month"] >= start_date)
                                   & (df_with_month["month"] <= end_date)))
    df_counts = df_new.groupBy(groupby, "month").count()
    df_counts = df_counts.withColumn(name+"_odd",
                                     when(df_counts["count"] <= threshold, 0)
                                     .otherwise(1))
    df_counts = df_counts.drop("count")
    return df_counts
