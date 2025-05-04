import logging
import csv
import random
import uuid
from datetime import date, datetime, timedelta

import duckdb
from faker import Faker
import polars as pl

from airflow import DAG
from airflow.operators.python import PythonOperator

def _create_data(locale: str) -> Faker:
    logging.info(f"Created synthetic data for {locale.split('_')[-1]} country code.")
    return Faker(locale)

def _generate_record(fake: Faker) -> list:
    person_name = fake.name()
    user_name = person_name.replace(" ", "").lower()
    email = f"{user_name}@{fake.free_email_domain()}"
    personal_number = fake.ssn()
    birth_date = fake.date_of_birth()
    address = fake.address().replace("\n", ", ")
    phone_number = fake.phone_number()
    mac_address = fake.mac_address()
    ip_address = fake.ipv4()
    iban = fake.iban()
    accessed_at = fake.date_time_between("-1y")
    session_duration = random.randint(0, 36_000)
    download_speed = random.randint(0, 1_000)
    upload_speed = random.randint(0, 800)
    consumed_traffic = random.randint(0, 2_000_000)

    return [
        person_name, user_name, email, personal_number, birth_date,
        address, phone_number, mac_address, ip_address, iban, accessed_at,
        session_duration, download_speed, upload_speed, consumed_traffic
    ]

def _write_to_csv() -> None:
    fake = _create_data("ro_RO")
    headers = [
        "person_name", "user_name", "email", "personal_number", "birth_date", "address",
        "phone", "mac_address", "ip_address", "iban", "accessed_at",
        "session_duration", "download_speed", "upload_speed", "consumed_traffic"
    ]

    rows = 100_372 if str(date.today()) == "2024-09-23" else random.randint(0, 1_101)

    with open("/opt/airflow/data/raw_data.csv", mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for _ in range(rows):
            writer.writerow(_generate_record(fake))
    logging.info(f"Written {rows} records to the CSV file.")

def _add_id_and_update_accessed_at():
    df = pl.read_csv("/opt/airflow/data/raw_data.csv")
    df = df.with_columns([
        pl.Series("unique_id", [str(uuid.uuid4()) for _ in range(df.height)])
    ])

    if str(date.today()) != "2024-09-23":
        yesterday = datetime.now().replace(microsecond=0) - timedelta(days=1)
        df = df.with_columns([
            pl.lit(str(yesterday)).alias("accessed_at")
        ])

    df.write_csv("/opt/airflow/data/raw_data.csv")
    logging.info("UUID added and accessed_at updated.")

def _save_to_duckdb():
    con = duckdb.connect(database="/opt/airflow/data/raw.duckdb", read_only=False)
    con.execute("CREATE SCHEMA IF NOT EXISTS driven_raw;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data AS 
        SELECT * FROM read_csv_auto('/opt/airflow/data/raw_data.csv', HEADER=TRUE)
    """)
    logging.info("Data loaded to DuckDB.")

def run_data_generation():
    logging.info("Started batch processing.")
    _write_to_csv()
    _add_id_and_update_accessed_at()
    _save_to_duckdb()
    logging.info("Finished batch processing.")


# Define DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    dag_id="duckdb_data_generator",
    default_args=default_args,
    start_date=datetime(2024, 9, 22),
    catchup=False,
    schedule_interval="@daily",
    description="Generate raw CSV data and load into DuckDB",
)

generate_raw_data = PythonOperator(
    task_id="generate_raw_data",
    python_callable=_write_to_csv,
    dag=dag,
)

add_ids_and_fix_timestamps = PythonOperator(
    task_id="add_ids_and_fix_timestamps",
    python_callable=_add_id_and_update_accessed_at,
    dag=dag,
)

save_to_duckdb = PythonOperator(
    task_id="save_to_duckdb",
    python_callable=_save_to_duckdb,
    dag=dag,
)

generate_raw_data >> add_ids_and_fix_timestamps >> save_to_duckdb