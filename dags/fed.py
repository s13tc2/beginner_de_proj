import os
import json
from datetime import datetime, timedelta

from utils.utils import _local_to_s3

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from config import CFG

os.environ['AWS_ACCESS_KEY_ID'] = CFG.AWS_ACCESS_KEY_ID
os.environ['AWS_SECRET_ACCESS_KEY'] = CFG.AWS_SECRET_ACCESS_KEY
os.environ['AWS_REGION'] = "us-east-1"

# Config
BUCKET_NAME = Variable.get("BUCKET")
print('BUCKET_NAME: ', BUCKET_NAME)

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
        "key": "raw/financial_data/{{ ds }}/sp500.csv",
        "bucket_name": BUCKET_NAME,
    },
)

sp500_to_raw_data_lake