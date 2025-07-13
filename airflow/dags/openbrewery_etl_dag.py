from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from src.config import ENV

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="openbrewery_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["bronze", "silver", "gold"]
) as dag:

    if ENV == "development":
        python_cmd = "source .venv/bin/activate && python3"
    else:
        python_cmd = "python3"  # Em produção assume que as libs já estão disponíveis

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"{python_cmd} src/bronze/openbrewerydb.py"
    )

    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command=f"{python_cmd} src/silver/mtd/breweries.py"
    )

    gold_aggregation = BashOperator(
        task_id="gold_aggregation",
        bash_command=f"{python_cmd} src/gold/brewery_per_type_and_location.py"
    )

    bronze_ingestion >> silver_transformation >> gold_aggregation