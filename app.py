from pyspark.sql import SparkSession
import streamlit as st
from rules_static import get_odds

def create_spark_instance():
    return (SparkSession
            .builder
            .appName("Data Pipeline")
            .getOrCreate())

def read_csv_file(spark, path):
    return (spark
            .read
            .options(header='True', inferSchema='True')
            .csv(path))

spark = create_spark_instance()
df_moves = read_csv_file(spark, "local_data/all_moves.csv")
df_momo_moov = read_csv_file(spark, "local_data/momo_moov.csv")
df_momo_mtn = read_csv_file(spark, "local_data/momo_mtn.csv")
df_momo = df_momo_moov.union(df_momo_mtn)

tooltip=['left:N', 'right:N', 'probe_id:N', 'diagram:N', 'count:N']
add_selectbox = st.sidebar.markdown("<h1 style='text-align: center; color: black;'>OWLSIFY</h1>", unsafe_allow_html=True)
col1, col2, col3 = st.columns(3)

with col1:
    kind = st.selectbox('Search by', ['Nomads', 'Fraudster'])
with col2:
    start_date = str(st.date_input('From'))
with col3:
    end_date = str(st.date_input('To'))

# Response moves dataframe
df_mv = get_odds('moves', df_moves, 'Noms', start_date[:-3], end_date[:-3], 2)
# Response momo dataframe
df_mm = get_odds('fraudster', df_momo, 'relative_to', start_date[:-3], end_date[:-3], 2)

if kind == 'Nomads':
    st.dataframe(df_mm, width=800, height=300)
elif kind == 'Fraudster':
    st.dataframe(df_mv, width=800, height=300)
