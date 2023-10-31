import requests
import json
import pandas as pd
import psycopg2

from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import timedelta
from pendulum import datetime

def _fetch_data(url: str):
    print(f'Extracting data: {url}')
    resp = requests.get(url)
    data_dump: list = []

    if resp.status_code == 200:
        resp = resp.json()

        next = resp.get('info', {}).get('next', None)
        data_dump.extend(resp.get('results', []))

        if bool(next):
            results = _fetch_data(next)
            data_dump.extend(results)

    print(f'Extraction done: {url}')
    return data_dump

def extract(folder: str):
    data_dump = _fetch_data('https://rickandmortyapi.com/api/character')
    with open(f'/tmp/{folder}/data_dump_test.json', 'w+') as f:
        json.dump(data_dump, f)

def transform(folder: str):
    data: list = []
    
    with open(f'/tmp/{folder}/data_dump_test.json', 'r+') as f:
        data = f.read()
        data = json.loads(data)

    data = [{
        'id': p['id'],
        'name': p['name'],
        'status': p['status'],
        'species': p['species'],
        'type': p['type'],
        'gender': p['gender'],
        'origin': p['origin']['name'],
        'location': p['location']['name'],
        'image': p['image'],
        'url': p['url'],
        'created': p['created'],
    } for p in data]

    df = pd.DataFrame(data)
    df.to_csv(f'/tmp/{folder}/data_dump.csv', index=False, header=False)

def load(folder: str):
    conn = psycopg2.connect('postgresql://airflow:airflow@postgres/postgres')
    cursor = conn.cursor()
    
    f = open(f'/tmp/{folder}/data_dump.csv', 'r')
    try:
        cursor.copy_from(f, 'characters', sep=',')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

with DAG(
    dag_id='09_python_etl',
    description='Basic Python ETL Tutorial',
    schedule='@once',
    start_date=datetime(2023,10,27),
    catchup=False,
    tags=['example', 'etl', 'python', 'bash'],
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

    t_extract = PythonOperator(
        task_id='t_extract',
        python_callable=extract,
        op_kwargs={'folder': dag.dag_id}
    )

    t_transform = PythonOperator(
        task_id='t_transform',
        python_callable=transform,
        op_kwargs={'folder': dag.dag_id}
    )

    t_load = PythonOperator(
        task_id='t_load',
        python_callable=load,
        op_kwargs={'folder': dag.dag_id}
    )

    t_cleanup = BashOperator(
        task_id='t_cleanup',
        bash_command=f'rm -rf /tmp/{dag.dag_id}'
    )

    t_start >> t_prepare >> t_extract >> t_transform >> t_load >> t_cleanup >> t_end