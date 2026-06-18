FROM apache/airflow:2.9.2-python3.11

USER root

# Dependências de sistema para httpx HTTP/2
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Criar diretórios da Bronze Layer
RUN mkdir -p /opt/airflow/data/bronze/chess_com \
             /opt/airflow/data/bronze/lichess \
    && chown -R airflow:root /opt/airflow/data

USER airflow

# Copiar e instalar dependências Python
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# PYTHONPATH para os módulos src/
ENV PYTHONPATH="/opt/airflow/src:${PYTHONPATH}"
