import pandas as pd
import os
import json
import logging

# Configurar logging
logger = logging.getLogger(__name__)

DATA_DIR = "/opt/airflow/dags/data"

def process_data(data):
    """
    Recebe dados JSON e transforma em DataFrame.
    Filtra apenas tasks completas (completed = True) e remove linhas incompletas.
    """
    try:
        df = pd.json_normalize(data)
        
        # Renomear colunas para compatibilidade
        df.rename(columns={"userId": "user_id"}, inplace=True)

        # Filtrar apenas tasks COMPLETAS (completed = True)
        df = df[df["completed"] == True]
        logger.info(f"Filtrado {len(df)} tasks completas")

        # Remove linhas com campos nulos
        df = df.dropna(subset=["user_id", "id", "title", "completed"])

        # Remove linhas com título vazio
        df = df[df["title"].str.strip() != ""]

        logger.info(f"Dataset final após limpeza: {len(df)} linhas")
        return df
        
    except Exception as e:
        logger.error(f"Erro no processamento dos dados: {str(e)}")
        raise

def transform_and_save():
    """
    Lê o arquivo JSON bruto, transforma os dados e salva como CSV.
    """
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        
        with open(f"{DATA_DIR}/raw_data.json", "r") as f:
            data = json.load(f)

        df = process_data(data)
        
        # Salvar dados processados
        df.to_csv(f"{DATA_DIR}/processed_data.csv", index=False)
        logger.info(f"Dados transformados salvos em {DATA_DIR}/processed_data.csv")
        
    except Exception as e:
        logger.error(f"Erro no processo de transformação: {str(e)}")
        raise
    