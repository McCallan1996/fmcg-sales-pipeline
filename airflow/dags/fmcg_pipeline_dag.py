from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
sys.path.insert(0, "/opt/airflow/scripts")

from download_dataset import download
from upload_to_gcs import upload
from gcs_to_bigquery import load
from run_spark_job import submit


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 3, 28),
}

with DAG(
    dag_id="fmcg_daily_sales_pipeline",
    default_args=default_args,
    description="End-to-end FMCG sales pipeline: Kaggle -> GCS -> BQ -> Spark -> dbt",
    schedule="@daily",
    catchup=False,
    tags=["fmcg", "batch"],
) as dag:

    download_task = PythonOperator(
        task_id="download_from_kaggle",
        python_callable=download,
    )

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload,
    )

    load_bq_task = PythonOperator(
        task_id="load_to_bigquery_raw",
        python_callable=load,
    )

    spark_task = PythonOperator(
        task_id="spark_transform",
        python_callable=submit,
    )

    dbt_task = BashOperator(
        task_id="run_dbt",
        bash_command="cd /opt/dbt && dbt deps && dbt build --profiles-dir . --target prod",
    )

    # pipeline flow
    download_task >> upload_task >> load_bq_task >> spark_task >> dbt_task
