# 🛠️ **Data Processing Pipelines with Airflow 3.0.0**

Welcome to the repository where data flows seamlessly through Bronze, Silver, and Golden layers. Powered by Apache Airflow 3.0.0, this project showcases modern ETL practices with a touch of elegance.

---

## 🚀 **What's New?**

### 🔧 **Airflow 3.0.0 Upgrade**

- **Modern Architecture**: Embracing the latest features and improvements in Airflow 3.0.0 to enhance performance and scalability.

- **TaskFlow API**: Utilizing the TaskFlow API with decorators for cleaner and more maintainable DAG definitions.

### 🧠 **SQL Logic Integration**

- **Unique ID & Date Management**: Moved the logic for inserting unique IDs and updating dates directly into SQL models, streamlining the ETL process.

### 🗄️ **Data Storage Strategy**

- **DuckDB**: Serving as the storage solution for Bronze and Silver layers, providing efficient data processing capabilities.

- **PostgreSQL**: Handling the Golden layer, ensuring reliable and scalable data storage for production use.

### 🧩 **DAGs Overview**

- **`duck_dbt_tag`**: A DAG dedicated to running DBT models in DuckDB, facilitating data transformation and analysis.

- **`raw_generation_dag`**: A DAG responsible for generating raw data, forming the foundation of the data pipeline.

Both DAGs are defined using Airflow decorators, following best practices for readability and maintainability.

### 🧊 **Polars Compatibility**

- **Existing DAG Update**: The existing DAG for Polars has been updated to ensure compatibility with the latest library versions, maintaining smooth data processing workflows.

---

## 🧪 **Example Usage**

```python
from airflow.decorators import dag, task
import pendulum

@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 5, 7, tz="Europe/Kyiv"),
    catchup=False,
    tags=["data_misc", "pii"],
)
def raw_generation_dag():
    @task
    def extract():
        pass

    @task
    def transform():
        pass

    @task
    def load():
        pass

    extract() >> transform() >> load()
