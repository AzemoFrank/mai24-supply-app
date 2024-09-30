from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# Définir le chemin vers les scripts
script_path = "scripts"  # Chemin dans le conteneur

with DAG(
    'supply_app_dag',
    default_args={'owner': 'airflow', 'start_date': datetime(2024, 1, 1)},
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Tâche pour le scraping
    task_scrapper = DockerOperator(
        task_id='scrapper_duckdb',
        image='schedul_supply_app',
        auto_remove=True,
        command=f'python {script_path}/scrapper_duckdb.py',
        # volumes=['./data:/data'],
        # docker_url='unix://var/run/docker.sock',
        # network_mode='bridge',
    )

    # Tâche pour nettoyer les données (clean_a)
    task_clean_a = DockerOperator(
        task_id='clean_a_duckdb',
        image='schedul_supply_app',
        auto_remove=True,
        command=f'python {script_path}/clean_a_duckdb.py',
        # volumes=['./data:/data'],
        # docker_url='unix://var/run/docker.sock',
        # network_mode='bridge',
    )

    # Tâche pour nettoyer les données (clean_b)
    task_clean_b = DockerOperator(
        task_id='clean_b_duckdb',
        image='schedul_supply_app',
        auto_remove=True,
        command=f'python {script_path}/clean_b_duckdb.py',
        # volumes=['./data:/data'],
        # docker_url='unix://var/run/docker.sock',
        # network_mode='bridge',
    )

    # Tâche pour entraîner le modèle
    task_train = DockerOperator(
        task_id='train_duckdb',
        image='schedul_supply_app',
        auto_remove=True,
        command=f'python {script_path}/train_duckdb.py',
        # volumes=['./data:/data'],
        # docker_url='unix://var/run/docker.sock',
        # network_mode='bridge',
    )

    # Définir l'ordre des tâches
    task_scrapper >> task_clean_a >> task_clean_b >> task_train