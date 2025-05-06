import pendulum
from airflow.sdk import dag

from airflow.pii.dags.duckdb.tasks.generate_data_task import generate_raw_data_and_write_to_csv
from airflow.pii.dags.duckdb.tasks.run_golden_dbt_task import run_dbt_golden_task
from airflow.pii.dags.duckdb.tasks.run_silver_dbt_task import run_dbt_silver_task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

@dag(
    dag_id="duckdb_data_generator_and_dbt_transformations",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 9, 22, tz="UTC"),
    catchup=False,
    description="Generate raw data and run dbt transformations (silver and golden models)",
    tags=["data-generation", "dbt", "silver", "golden"]
)
def main_dag():
    data_csv = "/opt/airflow/data/raw_data.csv"

    # Task dependencies
    generate_raw_data_and_write_to_csv(data_csv) >> run_dbt_silver_task() >> run_dbt_golden_task()

# Instantiate the DAG
main_dag()
