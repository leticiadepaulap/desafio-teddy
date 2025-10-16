import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
import pandas as pd
from typing import Tuple, Optional

# Configurar logging
logger = logging.getLogger(__name__)

# Carrega variáveis de ambiente
load_dotenv(dotenv_path="/opt/airflow/.env")

# Configurações
DATA_DIR = "/opt/airflow/dags/data"

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
    }
    
    missing_vars = [var_name for var_name, var_value in required_vars.items() if not var_value]
    
    if missing_vars:
        logger.error(f"Variaveis de ambiente faltando: {missing_vars}")
        return False
    
    logger.info("Todas as variaveis de ambiente validadas")
    return True

def create_db_engine() -> Optional[create_engine]:
    """Cria e retorna a engine de conexão com o banco"""
    try:
        if not validate_env_variables():
            return None
            
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        engine = create_engine(
            connection_string,
            connect_args={"options": f"-c search_path={schema}"},
            pool_pre_ping=True,
            echo=False
        )
        
        # Testar conexão
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            
        logger.info("Engine de banco de dados criada com sucesso")
        return engine
        
    except Exception as e:
        logger.error(f"Erro ao criar engine do banco: {str(e)}")
        return None

# Engine global
engine = create_db_engine()

def test_connection() -> bool:
    """
    Testa a conexão com o banco de dados
    
    Returns:
        bool: True se conexão bem sucedida
    """
    try:
        if engine is None:
            logger.error("Engine não inicializada")
            return False
            
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            db_version = result.fetchone()
            logger.info(f"Conectado ao PostgreSQL: {db_version[0]}")
            
            # Verificar se schema existe
            schema_check = conn.execute(
                text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema"),
                {"schema": schema}
            )
            
            if schema_check.fetchone():
                logger.info(f"Schema '{schema}' encontrado")
            else:
                logger.warning(f"Schema '{schema}' não encontrado")
                
        return True
        
    except Exception as e:
        logger.error(f"Falha no teste de conexão: {str(e)}")
        return False

def create_schema_if_not_exists() -> bool:
    """
    Cria o schema se não existir
    
    Returns:
        bool: True se schema criado ou já existia
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.commit()
            logger.info(f"Schema '{schema}' verificado/criado")
            return True
            
    except Exception as e:
        logger.error(f"Erro ao criar schema: {str(e)}")
        return False

def get_connection_stats() -> dict:
    """
    Retorna estatísticas da conexão e dados
    
    Returns:
        dict: Estatísticas
    """
    try:
        if engine is None:
            return {"status": "Engine não disponível"}
            
        with engine.connect() as conn:
            # Informações do banco
            result = conn.execute(text("""
                SELECT 
                    version() as db_version,
                    current_database() as db_name,
                    current_user as db_user
            """))
            db_info = result.fetchone()
            
            # Estatísticas das tabelas no schema
            tables_result = conn.execute(text("""
                SELECT 
                    table_name,
                    (xpath('/row/cnt/text()', query_to_xml(
                        format('SELECT COUNT(*) as cnt FROM %I.%I', table_schema, table_name), 
                        true, false, '')))[1]::text::int as row_count
                FROM information_schema.tables 
                WHERE table_schema = :schema
            """), {"schema": schema})
            
            tables_stats = {row[0]: row[1] for row in tables_result}
            
            return {
                "status": "Conectado",
                "db_version": db_info[0],
                "db_name": db_info[1],
                "db_user": db_info[2],
                "schema": schema,
                "tables": tables_stats
            }
            
    except Exception as e:
        return {"status": f"Erro: {str(e)}"}


"""
def load_data() -> Tuple[bool, int]:
    ""
    Lê o CSV processado e insere os dados no banco PostgreSQL.
    Apenas insere tasks completas (completed = True).
    
    Returns:
        Tuple[bool, int]: (sucesso, linhas_inseridas)
    ""
    try:
        if engine is None:
            logger.error("Engine de banco não disponível")
            return False, 0
        
        # Verificar/criar schema
        if not create_schema_if_not_exists():
            return False, 0
        
        # Ler dados processados
        csv_path = f"{DATA_DIR}/processed_data.csv"
        if not os.path.exists(csv_path):
            logger.error(f"Arquivo processado não encontrado: {csv_path}")
            return False, 0
            
        df = pd.read_csv(csv_path)
        
        if df.empty:
            logger.warning("DataFrame vazio, nada para carregar")
            return True, 0
        
        # Validar se todas as colunas necessárias estão presentes
        required_columns = ['user_id', 'id', 'title', 'completed']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Colunas faltando no DataFrame: {missing_columns}")
            return False, 0
        
        # Verificar se há dados duplicados baseado no ID
        duplicates = df.duplicated(subset=['id']).sum()
        if duplicates > 0:
            logger.warning(f"Encontrados {duplicates} IDs duplicados, removendo...")
            df = df.drop_duplicates(subset=['id'])
        
        # Carregar dados
        rows_before = get_row_count("todos")
        
        df.to_sql(
            "todos", 
            engine, 
            if_exists="append", 
            index=False, 
            schema=schema,
            method='multi'
        )
        
        rows_after = get_row_count("todos")
        rows_inserted = rows_after - rows_before
        
        logger.info(f"Dados carregados com sucesso: {rows_inserted} novas linhas inseridas")
        logger.info(f"Total de linhas na tabela: {rows_after}")
        
        return True, rows_inserted
        
    except Exception as e:
        logger.error(f"Erro ao carregar dados: {str(e)}")
        return False, 0

def get_row_count(table_name: str) -> int:
    ""
    Retorna o número de linhas em uma tabela
    
    Args:
        table_name: Nome da tabela
        
    Returns:
        int: Número de linhas
    ""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            )
            return result.scalar()
    except Exception as e:
        logger.error(f"Erro ao contar linhas: {str(e)}")
        return 0
"""

# Teste rápido se executado diretamente
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Testando modulo de loading...")
    
    if test_connection():
        print("Teste de conexão bem-sucedido!")
        stats = get_connection_stats()
        print(f"Estatisticas: {stats}")
    else:
        print("Falha no teste de conexão!")