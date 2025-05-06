import csv
import logging
import random
from datetime import date, datetime

from airflow.models import Variable

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from faker import Faker

def _create_data(locale: str) -> Faker:
    logging.info(f"Created synthetic data for {locale.split('_')[-1]} country code.")
    return Faker(locale)

def _generate_raw_data(fake: Faker) -> list:
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

def _generate_raw_data_and_write_to_csv(data_csv: str) -> None:
    fake = _create_data("ro_RO")
    headers = ["person_name", "user_name", "email", "personal_number", "birth_date", "address",
               "phone", "mac_address", "ip_address", "iban", "accessed_at",
               "session_duration", "download_speed", "upload_speed", "consumed_traffic"
               ]

    rows = 100_372 if str(date.today()) == "2024-09-23" else random.randint(0, 1_101)

    with open(data_csv, mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for _ in range(rows):
            writer.writerow(_generate_raw_data(fake))
    logging.info(f"Written {rows} records to the CSV file.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}
#
dag = DAG(
    dag_id="duckdb_data_generator",
    default_args=default_args,
    start_date=datetime(2024, 9, 22),
    catchup=False,
    description="Generate raw CSV data",
)

generate_raw_data = PythonOperator(
    task_id="generate_raw_data_and_write_to_csv",
    python_callable=_generate_raw_data_and_write_to_csv,
    op_kwargs={'data_csv': Variable.get("RAW_DATA_PATH", "Not Found")},
    dag=dag,
)