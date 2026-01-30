FROM apache/airflow:2.7.3-python3.10

USER root

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    pandas==2.0.3 \
    psycopg2-binary==2.9.9
