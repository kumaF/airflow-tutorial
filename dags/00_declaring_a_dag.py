from airflow import DAG

from airflow.operators.empty import EmptyOperator

from datetime import timedelta
from pendulum import datetime

with DAG(
    dag_id='00_declaring_a_dag',
    description='Basic DAG Tutorial',
    schedule='@once',
    start_date=datetime(2023,10,27, tz='UTC'),
    catchup=False,
    tags=['example'],
    default_args={
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(seconds=30),
        'do_xcom_push': False
    }
) as dag:
    
    t_start = EmptyOperator(task_id='t_start')
    t_end = EmptyOperator(task_id='t_end')

    t_01 = EmptyOperator(task_id='task_01')
    t_02 = EmptyOperator(task_id='task_02')
    t_03 = EmptyOperator(task_id='task_03')
    t_04 = EmptyOperator(task_id='task_04')

    t_start >> t_01 >> t_02 >> t_03 >> t_04 >> t_end