# 🛠️ Airflow 3.0.0 Pipelines - DAG examples with DBT

ETL workflows processing data across Bronze, Silver, and Golden layers using Airflow 3.0.0.
<img width="1491" alt="image" src="https://github.com/user-attachments/assets/36594344-7c38-4c50-9692-80249c5caacf" />

---

## 🔧 What's New

- **Airflow 3.0.0**: Upgraded to the latest version with enhanced features and performance.

- **SQL Logic**: Moved unique ID insertion and date updates directly into SQL models.

- **Data Storage**:
    - **DuckDB**: Bronze & Silver layers.
    - **PostgreSQL**: Golden layer.

- **DAGs**:
    - `duck_dbt_tag`: Runs DBT models in DuckDB and PostqreSQL
    - `raw_generation_dag`: Generates raw data.

Both DAGs are defined using Airflow decorators.

- **Polars DAG**: Existing DAG updated for compatibility with latest libraries.

---

## 📦 Useful Commands

1. Install local dependencies:

```bash
pip install -r requirements

```

2. Start Airflow 3.0.0 via Docker Compose
```bash
-> docker compose build
-> docker compose up airflow-init
-> docker compose up
-> docker compose down
-> docker compose down --volumes --rmi all
-> docker system prune --all
```

Check ENV variables in container
```bash
-> docker exec -it <container_id> env
-> docker exec -it <container_id> printenv DBT_PG_CONN

```

3. DBT
```bash
dbt run --target prod
```

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
