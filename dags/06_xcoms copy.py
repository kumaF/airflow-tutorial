from pendulum import datetime
from airflow import DAG

from airflow.utils.xcom import XCOM_RETURN_KEY
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta

def push_to_xcom(**kwargs):
    ti = kwargs['ti']
    task_id = kwargs['task_id']
    key = kwargs['key']

    data = ti.xcom_pull(
        task_ids=task_id,
        key=key
    )

    if isinstance(data, dict):
        for k, v in data.items():
            ti.xcom_push(
                key=k,
                value=v
            )
     
with DAG(
    dag_id='06_xcoms',
    description='XComs Tutorial',
    schedule='@once',
    start_date=datetime(2023,10,27, tz='UTC'),
    catchup=False,
    tags=['example', 'xcoms', 'http'],
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
        endpoint='/ip',
        method='GET',
        http_conn_id='http_httpbin',
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: response.json(),
        do_xcom_push=True,
    )

    t_python = PythonOperator(
        task_id='t_python',
        python_callable=push_to_xcom,
        provide_context=True,
        op_kwargs={
            'task_id': t_ping.task_id,
            'key': XCOM_RETURN_KEY
        }
    )

    t_email = EmailOperator(
        task_id='t_email',
        to=['mklmfernando@gmail.com'],
        subject='Endpoint health check',
        html_content="<h1>{{ ti.xcom_pull(task_ids='t_python', key='origin') }}</h1>",
    )

    t_start >> t_ping >> t_python >> t_email >> t_end