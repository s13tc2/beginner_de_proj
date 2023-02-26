import json
from datetime import datetime, timedelta

from utils.utils import _local_to_s3

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Config
BUCKET_NAME = Variable.get("BUCKET")

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2021, 5, 23),
    "end_date": datetime(2021, 5, 24),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "user_behaviour",
    default_args=default_args,
    max_active_runs=1,
)

sp500_to_raw_data_lake = PythonOperator(
    dag=dag,
    task_id="sp500_to_raw_data_lake",
    python_callable=_local_to_s3,
    op_kwargs={
        "file_name": "/opt/airflow/data/sp500.csv",
        "key": "raw/financial_data/{{ ds }}/movie.csv",
        "bucket_name": BUCKET_NAME,
    },
)

sp500_to_raw_data_lake