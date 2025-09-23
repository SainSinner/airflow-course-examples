from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago

dag = DAG(dag_id='AirFlow_6_1',  # Имя нашего дага, уникальное
          default_args={'owner': 'airflow'},  # Список необязательных аргументов
          schedule='@daily',  # Интервал запусков, в данном случае 1 раз в день 24:00
          start_date=datetime(2021, 1, 1)
          # Начальная точка запуска, это с какого момента мы бы хотели чтобы скрипт начал исполняться (далее разберем это подробнее)
          )

request_airflow_6 = SimpleHttpOperator(
    dag=dag,
    task_id="http_task",
    method="GET",  # Замените на 'POST', если нужно
    http_conn_id="http_random",  # Имя соединения, определенное в Airflow
    endpoint="/integers/?num=1&min=1&max=5&col=1&base=2&format=plain",  # Ваш endpoint
    headers={"Content-Type": "application/json"}
)
