import subprocess

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag, task
from pendulum import datetime

@dag(schedule="@daily",
     start_date=datetime(2025, 5, 7),
     catchup=False)
def duck_dbt_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    wait_for_data_gen = ExternalTaskSensor(
        task_id="wait_for_data_gen",
        external_dag_id="raw_generation_dag",
        external_task_id="generate_data_task",
        check_existence=True,
        timeout=10,
        poke_interval=5,
        mode="poke",
    )

    @task
    def run_dbt(command: str):
        subprocess.run(command, shell=True, check=True)

    dbt_run = "set -x; cd /opt/airflow/dbt && dbt run"
    dbt_commands = {
        "bronze": f"{dbt_run} --select tag:bronze",
        "silver": f"{dbt_run} --select tag:silver",
        "golden": f"{dbt_run} --select tag:golden"
    }

    bronze = run_dbt.override(task_id="run_bronze")(dbt_commands["bronze"])
    silver = run_dbt.override(task_id="run_silver")(dbt_commands["silver"])
    golden = run_dbt.override(task_id="run_golden")(dbt_commands["golden"])

    (start >> wait_for_data_gen >> bronze >> silver >> golden >> end)

dag_instance = duck_dbt_dag()
