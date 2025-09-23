import airflow
from airflow import DAG
from datetime import datetime
from airflow.models import BaseOperator
from airflow.providers.http.sensors.http import HttpSensor

import random

dag = DAG('AirFlow_6_3', schedule='@daily', start_date=datetime(2024, 1, 1))


def response_check(response, task_instance):
    # The task_instance is injected, so you can pull data form xcom
    # Other context variables such as dag, ds, execution_date are also available.
    # xcom_data = task_instance.xcom_pull(task_ids="http_sensor")
    # In practice you would do something more sensible with this data..
    # print(xcom_data)
    print(response.text)
    if int(response.text) == 5:
        print('Наконец нашли 5')
        return True


sensor = HttpSensor(
    task_id='http_sensor',
    http_conn_id='http_random',
    method="GET",  # Замените на 'POST', если нужно
    endpoint='/integers/?num=1&min=1&max=5&col=1&base=10&format=plain',
    response_check=response_check,
    poke_interval=3,
    timeout=60,
    dag=dag)
