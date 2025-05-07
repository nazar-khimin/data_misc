import csv
import logging
import random
from datetime import date

from airflow.sdk import task, dag
from faker import Faker
from pendulum import datetime

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

def generate_raw_data_and_write_to_csv(data_csv: str):
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
    logging.info(f"Written {rows} records to the CSV file {data_csv}")

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="raw_generation_dag")
def raw_generation_dag():
    @task(task_id="generate_data_task")
    def generate_data_task():
        file_path = "/opt/airflow/data/raw_data.csv"
        generate_raw_data_and_write_to_csv(file_path)
        return file_path

    generate_data_task()

dag_instance = raw_generation_dag()