## Overview

This repository contains **prototypes** related to data engineering.

## 📂 Topics

- 🌀 [Airflow](https://github.com/nazar-khimin/data_misc/tree/main/airflow) – ETL workflows processing data across Bronze, Silver, and Golden layers using Airflow 3.0.0
- 🧩 [Extended jaffle_shop_duckdb dbt playground](https://github.com/nazar-khimin/data_misc/tree/main/dbt/jaffle_shop_duckdb) – Runs dbt with DuckDB inside a Docker container, loading raw CSVs via SQL models to keep everything lightweight and local
- ☁️ [AWS](https://github.com/nazar-khimin/data_misc/tree/main/aws)  
  - 🔧 **ETL Pipeline using AWS Glue** – Synthetic data generation, schema extraction via Glue Crawlers, transformations with Glue Jobs, storage in S3/Data Catalog, and querying with Athena :contentReference 
  - ⚙️ **Automate AWS resource creation** – Terraform provisioning of S3, MWAA, Glue, IAM, etc., triggered by GitHub Actions
- ⚡️ [Kafka Consumer/Producer](https://github.com/nazar-khimin/data_misc/tree/main/kafka) – Publishes to and consumes from Kafka topics, persisting messages into PostgreSQL


## Sub-projects

**1.Airflow**  
ETL workflows processing data across Bronze, Silver, and Golden layers using Airflow 3.0.0.
<img width="1491" alt="image" src="https://github.com/user-attachments/assets/36594344-7c38-4c50-9692-80249c5caacf" />


**2. Extended jaffle_shop_duckdb dbt playground**

This project runs dbt with DuckDB inside a Docker container, without using dbt seed to load raw data. Instead, raw CSV files are loaded via SQL models as real DuckDB tables. The setup avoids unnecessary DB objects and keeps everything lightweight and local.
![lineage.png](dbt/jaffle_shop_duckdb/docs/lineage.png)
![dbt_components.png](dbt/jaffle_shop_duckdb/docs/components.png)

**3. AWS**

**3.1 ETL Pipeline using AWS Glue**
<img width="1449" alt="image" src="https://github.com/user-attachments/assets/d5dabb00-0362-4256-bedb-6f6c48f75c19" />


## AWS Services:
1. Amazon S3
2. AWS MWAA
3. AWS Glue Crawlers
4. AWS Glue Data Catalog
5. IAM
6. CloudWatch Logs
7. Amazon Athena

## 🧰 Tools & Technologies
1. Python
2. Terraform

## ⚙️ DAG Description

- Generate synthetic data  
- Store the data in an **S3**
- Scan data data in S3  
- Extract and register schema metadata using **Glue Crawler**
- Apply necessary transformations using AWS Glue Job
- Store data in S3 and metadata in **Glue Data Catalog**
- Query using **Athena**  

https://github.com/user-attachments/assets/b504542d-b802-4871-9632-86d5b61f80d0

**3.2 Automate AWS Resource creation using Terraform with GitHub Actions**

**4. Kafka Consumer/Producer with saving to Postqresql**

https://github.com/user-attachments/assets/5d1d1786-bfb4-4e5d-aec0-03fddad0986d