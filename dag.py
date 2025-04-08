from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from Load import run_loading

my_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retries_delay': timedelta(minutes=1)
}

dag = DAG(
    'vantage',
    default_args=my_args,
    schedule_interval='@weekly'  # specify interval weekly
)
load_task = PythonOperator(
    task_id='load',
    python_callable=run_loading,
    provide_context=True,
    dag=dag  # Assign the task to the DAG
)
load_task