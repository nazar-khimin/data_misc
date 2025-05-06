from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import task

@task
def run_dbt_golden_task() -> BashOperator:
    """
    Runs the dbt silver models.
    """
    return BashOperator(
        task_id='run_dbt_golden',
        bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:golden'
    )