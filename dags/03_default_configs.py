from pendulum import datetime
from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
    
with DAG(
    dag_id='03_default_configs',
    description='Setup Configs Tutorial',
    schedule='5 * * * *',
    start_date=datetime(2023,10,27, tz='UTC'),
    catchup=False,
    tags=['example', 'bash', 'setup_configs'],
    default_args={
        'depends_on_past': False,
        'email': ['mklmfernando@gmail.com'],
        'email_on_retry': False,
        'email_on_failure': False,
        'retries': 0,
        'retry_delay': timedelta(seconds=30),
        'do_xcom_push': False
    }
) as dag:
    
    t_start = EmptyOperator(task_id='t_start')
    t_end = EmptyOperator(task_id='t_end')

    t_welcome = BashOperator(
        task_id='t_welcome',
        bash_command='echo "Hello World"'
    )

    t_call = BashOperator(
        task_id='t_call',
        bash_command='echo "Kumara"'
    )

    t_sleep = BashOperator(
        task_id='t_sleep',
        bash_command='sleep "10"'
    )

    t_greet = BashOperator(
        task_id='t_greet',
        bash_command='echo "Bye"'
    )

    t_start >> t_welcome >> t_call >> t_sleep >> t_greet >> t_end