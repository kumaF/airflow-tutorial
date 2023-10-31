from pendulum import datetime
from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from datetime import timedelta
    
with DAG(
    dag_id='04_operators',
    description='Operator Tutorial',
    schedule='@once',
    start_date=datetime(2023,10,27, tz='UTC'),
    catchup=False,
    tags=['example', 'operator', 'setup_configs'],
    default_args={
        'depends_on_past': False,
        'email': ['mklmfernando@gmail.com'],
        'email_on_retry': False,
        'email_on_failure': False,
        'retries': 0,
        'retry_delay': timedelta(seconds=10),
        'do_xcom_push': False
    }
) as dag:
    
    t_start = EmptyOperator(task_id='t_start')
    t_end = EmptyOperator(task_id='t_end')

    t_ping = SimpleHttpOperator(
        task_id='t_ping',
        endpoint='/post',
        method='POST',
        http_conn_id='http_httpbin'
    )

    t_email = EmailOperator(
        task_id='t_email',
        to=['mklmfernando@gmail.com'],
        subject='Publish Status',
        html_content='<h1>Done</h1>'
    )

    t_start >> t_ping >> t_email >> t_end