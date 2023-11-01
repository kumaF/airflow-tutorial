from pendulum import datetime
from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.http.sensors.http import HttpSensor

from datetime import timedelta
    
with DAG(
    dag_id='05_sensors',
    description='Sensor Tutorial',
    schedule='@once',
    start_date=datetime(2023,10,27, tz='UTC'),
    catchup=False,
    tags=['example', 'sensor', 'setup_configs'],
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

    t_ping = HttpSensor(
        task_id='t_ping',
        endpoint='/get',
        method='GET',
        http_conn_id='http_httpbin',
        response_check=lambda response: response.status_code == 200,
        mode='poke', 
        timeout=300, 
        poke_interval=60,
    )

    t_email = EmailOperator(
        task_id='t_email',
        to=['mklmfernando@gmail.com'],
        subject='Endpoint health check',
        html_content='<h1>Healthy</h1>'
    )

    t_start >> t_ping >> t_email >> t_end