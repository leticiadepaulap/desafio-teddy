from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# Adiciona o diretório src ao path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Importa funções do pipeline
from extract import extract_and_save_data
from transform import transform_and_save
from loading import test_connection, load_data

# Define a DAG
with DAG(
    dag_id='api_data_pipeline_teddy',
    description='Pipeline de exemplo com extração, transformação e teste de conexão',
    start_date=datetime(2025, 10, 16),
    schedule_interval=None,
    catchup=False,
    tags=['teste', 'pipeline'],
) as dag:
    
    task_extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_and_save_data,
    )

    task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_and_save,
    )

    task_test_connection = PythonOperator(
    task_id='test_db_connection',
    python_callable=test_connection,
    )   

    task_load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    )

task_extract >> task_transform >> task_test_connection >> task_load
