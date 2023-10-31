import json

from pendulum import datetime
from airflow import DAG

from airflow.models.variable import Variable

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from datetime import timedelta

def process():
    foo = Variable.get('foo')
    bar = Variable.get('bar', 'default_bar')
    baz = Variable.get('baz', deserialize_json=True)
    secret = Variable.get_variable_from_secrets('secret')

    print(f'foo_val: {foo}')
    print(f'bar_val: {bar}')
    print(f'baz_val: {baz["baz"]}')
    print(f'secret_val: {secret}')

with DAG(
    dag_id='07_variables',
    description='Variables Tutorial',
    schedule='@once',
    start_date=datetime(2023,10,27, tz='UTC'),
    catchup=False,
    tags=['example', 'variables'],
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

    t_python = PythonOperator(
        task_id='t_python',
        python_callable=process
    )

    t_ping = SimpleHttpOperator(
        task_id='t_ping',
        endpoint=f'/base64/{Variable.get_variable_from_secrets("secret")}',
        method='GET',
        http_conn_id='http_httpbin',
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: response.text,
        do_xcom_push=True,
    )


    t_start >> t_python >> t_ping >> t_end