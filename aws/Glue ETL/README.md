
**ETL Pipeline using AWS Glue**

AWS Services:
1. Amazon S3
2. AWS MWAA
3. AWS Glue Crawlers
4. AWS Glue Data Catalog
5. IAM
6. CloudWatch Logs
7. Amazon Athena

For validation purposes:
1. automation-troubleshoot -> https://docs.aws.amazon.com/systems-manager-automation-runbooks/latest/userguide/automation-troubleshoot-mwaa-environment-creation.html
2. aws-support-tools verificator -> https://github.com/awslabs/aws-support-tools/tree/master/MWAA

🧰 Tools & Technologies
1. Python
2. Terraform

⚙️ DAG Description

### 1. ETL Transformation: Generate & Process Data

A **Glue Job** (PySpark-based) is triggered by **Airflow** to:

- Generate synthetic data  
- Apply necessary transformations  
- Output results in **Parquet** format  
- Store the data in an **S3** bucket under the `processed/` directory

---

### 2. Schema Crawling & Metadata Registration

A **Glue Crawler** is launched to:

- Scan the transformed data in S3  
- Extract and register schema metadata  
- Store the metadata in the **Glue Data Catalog**

This makes the data queryable via:

- **Athena**  
- **Redshift Spectrum**  
- Other compatible tools

<img width="1449" alt="image" src="https://github.com/user-attachments/assets/d5dabb00-0362-4256-bedb-6f6c48f75c19" />


https://github.com/user-attachments/assets/b504542d-b802-4871-9632-86d5b61f80d0

