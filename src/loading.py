import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from typing import Optional
from datetime import datetime
import pandas as pd
import json


# Configurar logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)

# Carrega variáveis de ambiente
load_dotenv(dotenv_path="/opt/airflow/.env")

# Variáveis do banco
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
db = os.getenv("DB_DATABASE")
schema = os.getenv("DB_SCHEMA")

def validate_env_variables() -> bool:
    """Valida se todas as variáveis de ambiente necessárias estão presentes"""
    required_vars = {
        "DB_USER": user,
        "DB_PASSWORD": password,
        "DB_HOST": host,
        "DB_PORT": port,
        "DB_DATABASE": db,
        "DB_SCHEMA": schema,
    }
    missing = [k for k, v in required_vars.items() if not v]
    if missing:
        logger.error(f"Variáveis de ambiente faltando: {missing}")
        return False
    logger.info("Todas as variáveis de ambiente estão presentes")
    return True

def create_db_engine() -> Optional[create_engine]:
    """Cria e testa a engine de conexão"""
    if not validate_env_variables():
        return None
    try:
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        engine = create_engine(
            conn_str,
            connect_args={"options": f"-c search_path={schema}"},
            pool_pre_ping=True,
            echo=False
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Conexão com banco estabelecida com sucesso")
        return engine
    except Exception as e:
        logger.error(f"Erro ao conectar com banco: {str(e)}")
        return None

def test_connection() -> bool:
    """Testa conexão e verifica se o schema existe"""
    engine = create_db_engine()
    if engine is None:
        return False
    try:
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version();")).scalar()
            logger.info(f"PostgreSQL versão: {version}")
            result = conn.execute(
                text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema"),
                {"schema": schema}
            )
            if result.fetchone():
                logger.info(f"Schema '{schema}' encontrado")
            else:
                logger.warning(f"Schema '{schema}' não encontrado")
        return True
    except Exception as e:
        logger.error(f"Erro ao testar conexão: {str(e)}")
        return False

def get_connection_stats() -> dict:
    """Retorna informações básicas da conexão"""
    engine = create_db_engine()
    if engine is None:
        return {"status": "Engine não disponível"}
    try:
        with engine.connect() as conn:
            info = conn.execute(text("""
                SELECT 
                    version() AS db_version,
                    current_database() AS db_name,
                    current_user AS db_user
            """)).fetchone()
            return {
                "status": "Conectado",
                "db_version": info[0],
                "db_name": info[1],
                "db_user": info[2],
                "schema": schema
            }
    except Exception as e:
        return {"status": f"Erro: {str(e)}"}
def insert_if_new(df: pd.DataFrame, table_name: str, id_column: str = "id") -> None:
    """
    Insere dados no banco apenas se ainda não existirem (com base no id).
    Adiciona coluna updated_at com timestamp atual.
    Funciona mesmo se a tabela estiver vazia.
    """
    engine = create_db_engine()
    if engine is None:
        logger.error("Engine indisponível. Abortando inserção.")
        return

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f'SELECT "{id_column}" FROM "{schema}"."{table_name}"')
            ).fetchall()

            existing_ids = [row[0] for row in result] if result else []

            novos = df[~df[id_column].isin(existing_ids)].copy()

            if novos.empty:
                logger.info("Nenhum dado novo para inserir.")
                return

            novos["updated_at"] = datetime.now()

            novos.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists="append",
                index=False
            )
            logger.info(f"{len(novos)} novos registros inseridos na tabela '{table_name}'.")

    except Exception as e:
        logger.error(f"Erro ao inserir dados: {str(e)}")

# Teste local
if __name__ == "__main__":
    print("Testando conexão com banco")
    if test_connection():
        print("Conexão OK")
        print(get_connection_stats())

        # Exemplo de uso do insert_if_new
        df_exemplo = pd.DataFrame([
            {"id": 1, "user_id": 1, "title": "delectus aut autem", "completed": False},
            {"id": 2, "user_id": 1, "title": "quis ut nam facilis", "completed": True},
        ])
        insert_if_new(df_exemplo, table_name="tarefas", id_column="id")
    else:
        print("Falha na conexão")


def load_data():
    """Lê os dados transformados e insere no banco"""
    df_path = "/opt/airflow/data/filtered_data.json"
    if not os.path.exists(df_path):
        logger.warning(f"Arquivo de dados não encontrado: {df_path}")
        return

    try:
        df = pd.read_json(df_path)
        insert_if_new(df, table_name="tarefas", id_column="id")
    except Exception as e:
        logger.error(f"Erro ao carregar dados: {str(e)}")

def save_data(data, filename="output.json"):
    """Salva dados em JSON no diretório de destino"""
    file_path = os.path.join(DATA_DIR, filename)
    with open(file_path, "w") as f:
        json.dump(data, f)
