from airflow.providers.standard.operators.bash import BashOperator

run_dbt_silver_task = BashOperator(
    task_id='run_dbt_staging',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:silver',
)