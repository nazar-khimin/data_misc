import subprocess

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task, dag, task_group
from pendulum import datetime

from generators.data_generator import generate_raw_data_and_write_to_csv

@task
def generate_data_task():
    file_path = "/opt/airflow/data/raw_data.csv"
    generate_raw_data_and_write_to_csv(file_path)

@task
def run_dbt(command: str):
    subprocess.run(command, shell=True, check=True)

@task_group(group_id="dbt_pipeline")
def dbt_pipeline():
    dbt_run = "set -x; cd /opt/airflow/dbt && dbt run"
    dbt_commands = {
        "bronze": f"{dbt_run} --select tag:bronze --target prod",
        "silver": f"{dbt_run} --select tag:silver --target prod",
        "golden": f"{dbt_run} --select tag:golden --target prod"
    }

    bronze = run_dbt.override(task_id="run_bronze")(dbt_commands["bronze"])
    silver = run_dbt.override(task_id="run_silver")(dbt_commands["silver"])
    golden = run_dbt.override(task_id="run_golden")(dbt_commands["golden"])
    return bronze >> silver >> golden

@dag(schedule=None,
     start_date=datetime(2025, 5, 7),
     catchup=False)
def pipeline_duckdb_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    prep = generate_data_task()
    dbt = dbt_pipeline()

    return start >> prep >> dbt >> end

dag_instance = pipeline_duckdb_dag()
