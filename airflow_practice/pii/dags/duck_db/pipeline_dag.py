import pendulum
from airflow.sdk import dag

from duck_db.run_data_generation_task import generate_raw_data_and_write_to_csv
from duck_db.run_golden_dbt_task import run_dbt_golden_task
from duck_db.run_silver_dbt_task import run_dbt_silver_task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

@dag(
    dag_id="duckdb_data_generator_and_dbt_transformations",
    schedule=None,
    default_args=default_args,
    start_date=pendulum.datetime(2025, 5, 1),
    catchup=False,
    description="Generate raw data and run dbt transformations (bronze, silver and golden models)",
    tags=["data-generation", "dbt", "silver", "golden"]
)
def main_dag():
    data_csv = "/opt/airflow/data/raw_data.csv"
    generate_raw_data_and_write_to_csv(data_csv) >> run_dbt_silver_task() >> run_dbt_golden_task()

main_dag()
