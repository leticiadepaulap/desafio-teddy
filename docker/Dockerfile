FROM apache/airflow:2.8.1-python3.11

USER airflow

# Instala apenas as dependências Python (não precisa do postgresql-client)
RUN pip install --no-cache-dir \
    pandas==2.1.4 \
    requests==2.31.0 \
    SQLAlchemy==1.4.49 \
    psycopg2-binary==2.9.7 \
    python-dotenv==1.0.0