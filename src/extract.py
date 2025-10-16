import requests
import os
import json

API_URL = "https://jsonplaceholder.typicode.com/todos"
DATA_DIR = "/opt/airflow/data"

def fetch_data(url=API_URL, params=None, headers=None):
    """Extrai dados de uma API e retorna em formato JSON"""
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()

def extract_and_save_data():
    """Função que extrai os dados da API e salva em JSON local"""
    os.makedirs(DATA_DIR, exist_ok=True)
    data = fetch_data()
    with open(f"{DATA_DIR}/raw_data.json", "w") as f:
        json.dump(data, f)
