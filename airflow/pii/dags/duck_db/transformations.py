import subprocess

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from pendulum import datetime

@dag(schedule=None,
     start_date=datetime(2025, 5, 7),
     catchup=False)
def duck_dbt_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def run_dbt(command: str):
        subprocess.run(command, shell=True, check=True)

    dbt_run = "set -x; cd /opt/airflow/dbt && dbt run"
    dbt_commands = {
        "bronze": f"{dbt_run} --select tag:bronze --target prod",
        "silver": f"{dbt_run} --select tag:silver --target prod",
        "golden": f"{dbt_run} --select tag:golden --target prod"
    }

    bronze = run_dbt.override(task_id="run_bronze")(dbt_commands["bronze"])
    silver = run_dbt.override(task_id="run_silver")(dbt_commands["silver"])
    golden = run_dbt.override(task_id="run_golden")(dbt_commands["golden"])

    (start >> bronze >> silver >> golden >> end)

dag_instance = duck_dbt_dag()
