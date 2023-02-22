from pyspark.sql.functions import substring, when, col, min, max
from calendar import monthrange
from datetime import datetime

def get_num_months(date1: str, date2: str) -> int:
    date_format = '%Y-%m'
    d1 = datetime.strptime(date1, date_format)
    d2 = datetime.strptime(date2, date_format)
    num_months = (d2.year - d1.year) * 12 + (d2.month - d1.month)
    return num_months

def get_odds_fixed(df, name_col, date_col, start_month=None, end_month=None,
                   threshold=5):
    min_date, max_date = None, None
    df_with_month = df.withColumn('month', substring(date_col, 1, 7))
    if start_month == None and end_month == None:
        min_date = df_with_month.select(min('month')).collect()[0][0]
        max_date = df_with_month.select(max('month')).collect()[0][0]
    else:
        min_date, max_date = start_date, end_date
    num_months = get_num_months(min_date, max_date)
    df_indices = (df_with_month
                 .groupBy(name_col, 'month')
                 .count()
                 .withColumn('odds', when(col('count') >= threshold, 1).otherwise(0))
                 .groupby(name_col)
                 .sum('odds')
                 .withColumn('indice', col('sum(odds)')/num_months)
    )
    return df_indices


"""df = (spark
      .read
      .option('header', 'true')
      .option('inferSchema', 'true')
      .csv('../data/moves.csv'))


get_odds_fixed(df, 'Nom', 'Date', '2013-01', '2022-01', 1)"""
