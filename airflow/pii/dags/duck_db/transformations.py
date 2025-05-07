import subprocess
from datetime import datetime

from airflow.sdk import dag, task

@dag(schedule=None, start_date=datetime(2023, 1, 1), catchup=False)
def dbt_dag():
    @task()
    def run_dbt(command: str):
        subprocess.run(command, shell=True, check=True)

    bronze_models = run_dbt('set -x; cd /opt/airflow/dbt && dbt run --select tag:bronze')
    silver_models = run_dbt('set -x; cd /opt/airflow/dbt && dbt run --select tag:silver')
    golden_models = run_dbt('set -x; cd /opt/airflow/dbt && dbt run --select tag:golden')

    bronze_models >> silver_models >> golden_models

dag_instance = dbt_dag()