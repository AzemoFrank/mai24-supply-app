from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'supply_chain_pipeline',
    default_args=default_args,
    description='Pipeline de la chaîne d\'approvisionnement',
    schedule_interval=timedelta(days=1),
)

# Définition des tâches avec BashOperator
scrapper_task = BashOperator(
    task_id='scrapper',
    bash_command='python /scripts/scrapper_duckdb.py',
    dag=dag,
)

clean_a_task = BashOperator(
    task_id='clean_a',
    bash_command='python /scripts/clean_a_duckdb.py',
    dag=dag,
)

clean_b_task = BashOperator(
    task_id='clean_b',
    bash_command='python /scripts/clean_b_duckdb.py',
    dag=dag,
)

train_task = BashOperator(
    task_id='train',
    bash_command='python /scripts/train_duckdb.py',
    dag=dag,
)

# Définition de l'ordre des tâches
scrapper_task >> clean_a_task >> clean_b_task >> train_task