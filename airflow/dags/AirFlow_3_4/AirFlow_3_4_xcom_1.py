import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

args = {'owner': 'airflow', 'start_date': days_ago(1), 'provide_context': True}
dag = DAG('AirFlow_3_4_xcom_1', schedule_interval='@once', default_args=args)

value_2 = {'a': 'b'}


def push(**kwargs):
    kwargs['ti'].xcom_push(key='key', value=value_2)


def pull(**kwargs):
    print(kwargs['ti'].xcom_pull(key='key', task_ids='push'))


push1 = PythonOperator(
    task_id='push', dag=dag, python_callable=push)
pull1 = PythonOperator(
    task_id='pull', dag=dag, python_callable=pull)

push1 >> pull1
