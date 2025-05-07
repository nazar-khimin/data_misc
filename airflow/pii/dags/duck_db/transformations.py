import subprocess
from datetime import datetime

from airflow.sdk import dag, task

@dag(schedule=None, start_date=datetime(2023, 1, 1), catchup=False)
def dbt_dag():
    @task
    def run_dbt(command: str):
        subprocess.run(command, shell=True, check=True)

    dbt_commands = {
        "bronze": "set -x; cd /opt/airflow/dbt && dbt run --select tag:bronze",
        "silver": "set -x; cd /opt/airflow/dbt && dbt run --select tag:silver",
        "golden": "set -x; cd /opt/airflow/dbt && dbt run --select tag:golden",
    }

    bronze = run_dbt.override(task_id="run_bronze")(dbt_commands["bronze"])
    silver = run_dbt.override(task_id="run_silver")(dbt_commands["silver"])
    golden = run_dbt.override(task_id="run_golden")(dbt_commands["golden"])

    bronze >> silver >> golden

dag_instance = dbt_dag()