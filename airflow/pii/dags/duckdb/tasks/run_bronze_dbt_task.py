from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import task

@task
def run_dbt_bronze_task() -> BashOperator:
    """
    Runs the dbt bronze models.
    """
    return BashOperator(
        task_id='run_dbt_bronze',
        bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:bronze'
    )