# airflow related
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

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

main_job = PythonOperator(
  task_id='Main_Job', 
  python_callable=sparkMain.myDriverFunction, 
  dag=dag
)
  
dummy_operator = DummyOperator(
    task_id='Starting_Task',
    retries = 3, 
    dag=dag
)

dummy_operator >> main_job
