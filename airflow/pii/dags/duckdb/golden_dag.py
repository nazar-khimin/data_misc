# Define golden dbt models run task.
from airflow.providers.standard.operators.bash import BashOperator

run_dbt_golden_task = BashOperator(
    task_id='run_dbt_golden',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:golden',
)