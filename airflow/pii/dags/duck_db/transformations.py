import subprocess
from datetime import datetime

from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag, task

@dag(schedule=None, start_date=datetime(2023, 1, 1), catchup=False)
def duck_dbt_dag():
    wait_for_data_gen = ExternalTaskSensor(
        task_id="wait_for_data_gen",
        external_dag_id="raw_generation_dag",
        external_task_id="generate_data_task",
        timeout=30,
        poke_interval=5,
        mode="poke",
    )

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

    wait_for_data_gen >> bronze >> silver >> golden

dag_instance = duck_dbt_dag()