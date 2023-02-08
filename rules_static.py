from pyspark.sql.functions import substring, when, col

def get_diff(date_1, date_2):
    year1, month1 = list(map(int, date_1.split('-')))
    year2, month2 = list(map(int, date_2.split('-')))
    return (year2-year1)*12 + month2-month1 + 1

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
    combdf = (df_counts
              .groupBy(groupby)
              .sum()
              .withColumnRenamed(f"sum({name}_odd)", "sum_odds"))
    combdf = combdf.withColumn("ratio", col("sum_odds")/get_diff(start_date, end_date))
    combdf = combdf.withColumn("level",
                               when((combdf["ratio"] >= 0) & (combdf["ratio"] <= 0.3), 'low')
                               .when((combdf["ratio"] > 0.3) & (combdf["ratio"] <= 0.6), 'medium')
                               .when((combdf["ratio"] > 0.6), 'high'))
    return combdf
