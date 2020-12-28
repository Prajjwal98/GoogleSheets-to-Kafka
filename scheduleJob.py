# airflow related
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import sparkMain

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '*/10 * * * *',                     # Scheduled to run every 10 mins.
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(dag_id='schedule_Kafka_Job', description='DAG for Kafka Assignment', default_args=default_args) as dag

main job = PythonOperator(
  task_id='main_job', 
  python_callable=sparkMain.myDriverFunction, 
  dag=dag
)
  
