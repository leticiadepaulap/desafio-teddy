from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Importação direta do módulo (sem 'src.')
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from extract import extract_and_save_data

with DAG(
    dag_id='dag_teste_importacao',
    start_date=datetime(2025, 10, 16),
    schedule_interval=None,
    catchup=False,
) as dag:

    tarefa_teste = PythonOperator(
        task_id='executar_funcao_simples',
        python_callable=extract_and_save_data,
    )
