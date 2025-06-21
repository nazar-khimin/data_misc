
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

**1. ETL Transformation: Generate & Process Data**
    -> A Glue Job (PySpark-based) is triggered from Airflow.
    -> It generates synthetic data, applies required transformations, and outputs the result in Parquet format to an S3 "processed" directory.
**2. Schema Crawling & Metadata Registration**
    -> A Glue Crawler scans the transformed S3 data.
    -> Schema metadata is stored in the Glue Data Catalog, making it queryable via Athena, Redshift Spectrum, or other tools.

<img width="1449" alt="image" src="https://github.com/user-attachments/assets/d5dabb00-0362-4256-bedb-6f6c48f75c19" />


https://github.com/user-attachments/assets/b504542d-b802-4871-9632-86d5b61f80d0

