from pendulum import datetime
from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import timedelta
    
with DAG(
    dag_id='02_email_alerts',
    description='Email Alerts Tutorial',
    schedule='@once',
    start_date=datetime(2023,10,27, tz='UTC'),
    catchup=False,
    tags=['example', 'bash', 'email_alerts'],
    default_args={
        'depends_on_past': False,
        'email': ['mklmfernando@gmail.com'],
        'email_on_retry': True,
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
        'do_xcom_push': False
    }
) as dag:
    
    t_start = EmptyOperator(task_id='t_start')
    t_end = EmptyOperator(task_id='t_end')

    t_bash_hello_world = BashOperator(
        task_id='t_bash_hello_world',
        bash_command='sleep "Hello World"'
    )

    t_start >> t_bash_hello_world >> t_end