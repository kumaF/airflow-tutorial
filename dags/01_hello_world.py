from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta
from pendulum import datetime

def hello_world():
    print('Hello World from Python')

with DAG(
    dag_id='01_hello_world',
    description='Hello World Tutorial',
    schedule='@once',
    start_date=datetime(2023,10,27, tz='UTC'),
    catchup=False,
    tags=['example', 'bash'],
    default_args={
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(seconds=30),
        'do_xcom_push': False
    }
) as dag:
    
    t_start = EmptyOperator(task_id='t_start')
    t_end = EmptyOperator(task_id='t_end')

    t_bash_hello_world = BashOperator(
        task_id='t_bash_hello_world',
        bash_command='echo "Hello World"'
    )

    t_python_hello_world = PythonOperator(
        task_id='t_python_hello_world',
        python_callable=hello_world
    )

    t_start >> t_bash_hello_world >> t_python_hello_world >> t_end