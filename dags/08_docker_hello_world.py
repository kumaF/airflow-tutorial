from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator

from datetime import timedelta
from pendulum import datetime


with DAG(
    dag_id='08_docker_hello_world',
    description='Docker Hello World Tutorial',
    schedule='@once',
    start_date=datetime(2023,10,27),
    catchup=False,
    tags=['example', 'docker', 'bash'],
    default_args={
        'depends_on_past': False,
        'email': ['mklmfernando@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'do_xcom_push': False
    }
) as dag:
    t_start = EmptyOperator(task_id='t_start')
    t_end = EmptyOperator(task_id='t_end')

    t_docker_hello_world = DockerOperator(
        task_id='t_docker_hello_world',
        network_mode='bridge',
        api_version='auto',
        auto_remove='success',
        image='ubuntu',
        container_name='ubuntu',
        command=[
            "/bin/bash",
            "-c",
            "/bin/echo 'Hello World';"
            "/bin/sleep 30;"
            "/bin/echo 'Bye';"
        ],
    )

    t_start >> t_docker_hello_world >> t_end