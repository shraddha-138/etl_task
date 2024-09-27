from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys
from pathlib import Path
sys.path.append("/home/shraddha/airflow/etl_pipeline")

# Import the etl_pipeline functions
from etl_pipeline import read, fill_nulls, calculate_running_total, write_to_hudi, get_spark_session 

def read_data(**kwargs):
    df = read()
    # Save the DataFrame to a temporary location, e.g., as a parquet file
    df.write.mode('overwrite').parquet('/tmp/read_data.parquet')
    # Return the file path instead of the DataFrame
    return '/tmp/read_data.parquet'

def transform_data(**kwargs):
    # Retrieve the file path from XCom
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='read_data')
    
    # Read the DataFrame from the parquet file
    spark = get_spark_session()
    df = spark.read.parquet(file_path)
    
    df_filled = fill_nulls(df)
    df_transformed = calculate_running_total(df_filled)
    
    # Save the transformed DataFrame to another temporary location
    df_transformed.write.mode('overwrite').parquet('/tmp/transformed_data.parquet')
    return '/tmp/transformed_data.parquet'

def load_data(**kwargs):
    # Retrieve the file path from XCom
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='transform_data')
    
    # Read the DataFrame from the parquet file
    spark = get_spark_session()
    df = spark.read.parquet(file_path)
    
    # Write the DataFrame to Hudi
    write_to_hudi(df, "/home/shraddha/data_csv3")

# DAG definition
with DAG(dag_id="etl_workflow_dag", start_date=datetime(2024, 8, 13), schedule_interval="@daily", catchup=False) as dag:
    
    read_task = PythonOperator(
        task_id="read_data",
        python_callable=read_data,
        do_xcom_push=True  # Now returning file path, not the DataFrame
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        do_xcom_push=True  
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        do_xcom_push=False  # We are not returning anything here
    )

    read_task >> transform_task >> load_task

