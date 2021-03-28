from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from covid_etl import run_covid_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0, 0, 0, 0, 0),
    'email': ['..@..com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'covidPR_dag',
    default_args=default_args,
    description='Covid PR',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='whole_covid_etl',
    python_callable=run_covid_etl,
    dag=dag,
)

run_etl
