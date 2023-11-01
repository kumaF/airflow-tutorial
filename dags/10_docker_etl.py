from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator

from docker.types import Mount
from datetime import timedelta
from pendulum import datetime

with DAG(
    dag_id='10_docker_etl',
    description='Basic Python ETL with Docker Tutorial',
    schedule='@once',
    start_date=datetime(2023,10,27),
    catchup=False,
    tags=['example', 'etl', 'python', 'docker', 'bash'],
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

    t_prepare = BashOperator(
        task_id='t_prepare',
        bash_command=f'mkdir /tmp/{dag.dag_id}'
    )

    t_extract = DockerOperator(
        task_id='t_extract',
        network_mode='bridge',
        api_version='auto',
        auto_remove='success',
        image='python_docker',
        container_name='t_extract',
        command=[
            "-p",
            "extract",
            "-f"
            f"{dag.dag_id}"
        ],
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source='/Users/kumarafernando/Desktop/airflow/tmp',
                target='/tmp',
                type='bind'
            ),
        ]
    )

    t_transform = DockerOperator(
        task_id='t_transform',
        network_mode='bridge',
        api_version='auto',
        auto_remove='success',
        image='python_docker',
        container_name='t_transform',
        command=[
            "-p",
            "transform",
            "-f"
            f"{dag.dag_id}"
        ],
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source='/Users/kumarafernando/Desktop/airflow/tmp',
                target='/tmp',
                type='bind'
            ),
        ]
    )

    t_load = DockerOperator(
        task_id='t_load',
        network_mode='host',
        api_version='auto',
        auto_remove='success',
        image='python_docker',
        container_name='t_load',
        command=[
            "-p",
            "load",
            "-f"
            f"{dag.dag_id}"
        ],
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source='/Users/kumarafernando/Desktop/airflow/tmp',
                target='/tmp',
                type='bind'
            ),
        ]
    )

    t_cleanup = BashOperator(
        task_id='t_cleanup',
        bash_command=f'rm -rf /tmp/{dag.dag_id}'
    )

    t_start >> t_prepare >> t_extract >> t_transform >> t_load >> t_cleanup >> t_end