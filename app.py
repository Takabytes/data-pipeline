import datetime
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, when
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
df_convictions = read_csv_file(spark, "local_data/convictions.csv")

tooltip=['left:N', 'right:N', 'probe_id:N', 'diagram:N', 'count:N']
#add_selectbox = st.sidebar.markdown("<h1 style='text-align: center; color: black;'>OWLSIFY</h1>", unsafe_allow_html=True)

def static():
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        kind = st.selectbox('Search by', ['Nomads', 'Fraudster'])
    with col2:
        start_date = str(st.date_input('From', min_value=datetime.date(2000, 1, 1)))
    with col3:
        end_date = str(st.date_input('To', min_value=datetime.date(2000, 1, 1)))
    with col4:
        thres = int(st.number_input('Threshold', min_value=0, step=1))
    # Response moves dataframe
    df_mv = get_odds('moves', df_moves, 'Noms', start_date[:-3], end_date[:-3], thres)
    # Response momo dataframe
    df_mm = get_odds('fraudster', df_momo, 'relative_to', start_date[:-3], end_date[:-3], thres)
    if kind == 'Nomads':
        st.dataframe(df_mm, 800, 300)
    elif kind == 'Fraudster':
        st.dataframe(df_mv, 800, 300)

def dynamic():
    savedPipelineModel = PipelineModel.load('rf-pipeline-model')
    predictions = savedPipelineModel.transform(df_convictions)
    cleanDF = predictions.select('Name', 'Age', 'prediction')
    cleanDF = cleanDF.withColumn("level", when(cleanDF['prediction'] == 1, "suspect").otherwise('no suspect'))
    st.dataframe(cleanDF, 100, 400)

page_names_to_funcs = {
    "STATIC": static,
    "DYNAMIC": dynamic
}

demo_name = st.sidebar.selectbox("Inference Type", page_names_to_funcs.keys())
page_names_to_funcs[demo_name]()
