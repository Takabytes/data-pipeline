"""
Example Airflow DAG to submit Apache Spark applications using
`SparkSubmitOperator`, `SparkJDBCOperator` and `SparkSqlOperator`.
"""
from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import datetime


with DAG(
    dag_id='example_spark_operator',
    schedule_interval="@hourly",
    start_date=datetime(2023,2,23),
    tags=['example'],
) as dag:
    # [START howto_operator_spark_submit]
    
    batch_job = SparkSubmitOperator(
        application="../../../source/damia_batch.py", task_id="batch_job", conn_id='spark_default'
    )
    
    move_to_working_job = SparkSubmitOperator(
        application="../../../source/move_files_to_working.py", task_id="move_to_working_job", conn_id='spark_default'
    )

    move_to_processed_job = SparkSubmitOperator(
        application="../../../source/move_files_to_processed.py", task_id="move_to_processed_job", conn_id='spark_default'
    )

    # [python_submit_job, scala_submit_job]
    batch_job >> move_to_working_job >> move_to_processed_job
    