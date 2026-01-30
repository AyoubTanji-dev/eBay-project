"""
eBay Morocco ETL Pipeline DAG
Orchestrates the daily data ingestion, transformation, and loading process.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ==============================================================================
# DAG Configuration
# ==============================================================================
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ebay_morocco_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for eBay Morocco market data',
    schedule_interval='@daily',  # Run once per day at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't backfill old runs
    tags=['ebay', 'etl', 'data-engineering'],
)

# ==============================================================================
# Task Definitions
# ==============================================================================

# Task 1: Simulate data arrival (extract batch from Silver reservoir)
simulate_arrival = BashOperator(
    task_id='simulate_data_arrival',
    bash_command='python /opt/airflow/scripts/simulate_arrival.py',
    dag=dag,
)

# Task 2: Transform to Gold (Star Schema)
transform_gold = BashOperator(
    task_id='transform_to_gold',
    bash_command='python /opt/airflow/scripts/transform_gold.py',
    dag=dag,
)

# Task 3: Load to PostgreSQL
load_postgres = BashOperator(
    task_id='load_to_postgres',
    bash_command='python /opt/airflow/scripts/load_postgres.py',
    dag=dag,
)

# Task 4: Cleanup (Optional - just a success marker)
pipeline_complete = BashOperator(
    task_id='pipeline_complete',
    bash_command='echo "✅ ETL Pipeline completed successfully at $(date)"',
    dag=dag,
)

# ==============================================================================
# Task Dependencies (The Pipeline Order)
# ==============================================================================
simulate_arrival >> transform_gold >> load_postgres >> pipeline_complete
