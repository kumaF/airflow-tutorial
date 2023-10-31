# from airflow import DAG

# from airflow.operators.empty import EmptyOperator
# from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# from datetime import (
#     datetime,
#     timedelta
# )

# with DAG(
#     dag_id='kube_hello_world',
#     description='Hello World Tutorial',
#     schedule='@once',
#     start_date=datetime(2023,10,27),
#     catchup=False,
#     tags=['example', 'python'],
#     default_args={
#         'depends_on_past': False,
#         'email': ['mklmfernando@gmail.com'],
#         'email_on_failure': True,
#         'email_on_retry': False,
#         'retries': 1,
#         'retry_delay': timedelta(minutes=5)
#     }
# ) as dag:
    
#     t_start = EmptyOperator(task_id='t_start')
#     t_end = EmptyOperator(task_id='t_end')

#     t_kube_hello_world = KubernetesPodOperator(
#         task_id='t_kube_hello_world',
#         namespace='default',
#         config_file='/opt/airflow/config/kubeconfig',
#         image='ubuntu',
#         cmds=[
#             "/bin/bash",
#             "-c",
#             "/bin/echo 'Hello World';"
#             "/bin/sleep 30;"
#             "/bin/echo 'Bye';"
#         ],
#     )

#     t_start >> t_kube_hello_world >> t_end