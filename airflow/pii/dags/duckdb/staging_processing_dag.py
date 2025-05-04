from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb

# Define DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    dag_id="duckdb_staging_processing",
    default_args=default_args,
    start_date=datetime(2024, 9, 22),
    catchup=False,
    schedule_interval="@daily",
    description="Staging layer transformation using DuckDB",
)

def staging_processing():
    conn = duckdb.connect("/opt/airflow/db/driven.duckdb")
    conn.execute("CREATE SCHEMA IF NOT EXISTS driven_staging;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS driven_staging.cleaned_batch_data AS
        SELECT * FROM driven_raw.raw_batch_data WHERE true;
    """)
    conn.execute("DELETE FROM driven_staging.cleaned_batch_data;")
    conn.execute("""
        INSERT INTO driven_staging.cleaned_batch_data
        SELECT
            unique_id,
            LOWER(email) AS email,
            accessed_at,
            session_duration,
            download_speed,
            upload_speed,
            consumed_traffic
        FROM driven_raw.raw_batch_data
        WHERE session_duration > 0;
    """)

staging_task = PythonOperator(
    task_id="staging_processing",
    python_callable=staging_processing,
    dag=dag,
)

if __name__ == "__main__":
    dag.test()
