from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain, cross_downstream

import os
from datetime import datetime, timedelta

os.environ['USER'] = 'Carlos Barbosa'

owner = os.environ.get('USER')


default_args = {
    'owner': owner,
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

def _downloading_data(**kwargs):
    with open('/tmp/my_file.txt', 'w') as f:
        f.write('my_data')

def _checking_data():
    print("Check data...")

with DAG(dag_id='simple_dag',
         default_args=default_args,
         schedule_interval=timedelta(minutes=1),
         start_date=datetime(2021, 3, 13),
         catchup=False
         ) as dag:
    
    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data
    )

    checking_data = PythonOperator(
        task_id="checking_data",
        python_callable=_checking_data
    )

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='my_file.txt',
        poke_interval=30
    )

    processing_data = BashOperator(
        task_id='processing_data',
        bash_command='cat /tmp/my_file.txt'
    )

    cross_downstream([downloading_data, checking_data], [waiting_for_data, processing_data])