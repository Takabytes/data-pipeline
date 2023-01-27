import streamlit as st
from spark_df import get_nomads, get_scammers
import matplotlib.pyplot as plt
import pandas as pd
from el_spark import get_schema_momo, get_schema_move, spark_reader

df_moves = spark_reader("hdfs://localhost:9000/datalake/all_moves.csv", get_schema_move())
df_momo = spark_reader("hdfs://localhost:9000/datalake/momo.csv", get_schema_momo())

tooltip=['left:N', 'right:N', 'probe_id:N', 'diagram:N', 'count:N']
add_selectbox = st.sidebar.markdown("<h1 style='text-align: center; color: black;'>ALLVISION</h1>", unsafe_allow_html=True)
col1, col2, col3, col4 = st.columns(4)
with col1:
    kind = st.selectbox('Pick the type', ['Nomads', 'Scammers'])
with col2:
    start_date = str(st.date_input('Start date'))
with col3:
    end_date = str(st.date_input('End date'))
with col4:
    choice = st.number_input("Threshold", 1, 50)

# Response moves dataframe
df_mv = get_nomads(df_moves, start_date, end_date, int(choice))
# Response momo dataframe
df_mm = get_scammers(df_momo, start_date, end_date, int(choice))

if kind == 'Nomads':
    st.dataframe(df_mm, width=800, height=300)
elif kind == 'Scammers':
    st.dataframe(df_mv, width=800, height=300)
