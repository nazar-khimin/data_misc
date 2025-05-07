import subprocess
from datetime import datetime

from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag, task

@dag(schedule=None, start_date=datetime(2023, 1, 1), catchup=False)
def duck_dbt_dag():

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

    bronze >> silver >> golden

dag_instance = duck_dbt_dag()