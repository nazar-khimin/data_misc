import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag

from run_data_generation_task import generate_raw_data_and_write_to_csv

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

    generate_raw_data_task = PythonOperator(
        task_id="generate_raw_data",
        python_callable=generate_raw_data_and_write_to_csv,
        op_args=[data_csv],
    )
    bronze_models = BashOperator(
        task_id='run_dbt_bronze',
        bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:bronze'
    )

    silver_models = BashOperator(
        task_id='run_dbt_silver',
        bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:silver'
    )

    golden_models = BashOperator(
        task_id='golden',
        bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:golden'
    )

    (
            generate_raw_data_task >>
            bronze_models >>
            silver_models >>
            golden_models
    )

main_dag()
