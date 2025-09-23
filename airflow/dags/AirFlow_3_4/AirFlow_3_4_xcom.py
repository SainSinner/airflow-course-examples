from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime


# Функция которая положит в Xcom некотрое значение
def push_function(**context):
    context['ti'].xcom_push(
        key='myFirstXcomKey', value="Это я проверяю запись данных в XCOM"
    )


# Функция которая извлечет это значение
def pull_function(**context):
    ti = context['ti']  # Получим из контекста экземпляр задачи
    value_pulled = ti.xcom_pull(
        key='myFirstXcomKey'
    )  # Через экземпляр обратимся по имени к Xcom
    print(value_pulled)


dag = DAG(
    'AirFlow_3_4_xcom',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 1, 10),
)

push_task = PythonOperator(
    task_id='push_task',
    python_callable=push_function,
    dag=dag,
)

pull_task = PythonOperator(
    task_id='pull_task',
    python_callable=pull_function,
    dag=dag,
)

# https://catfact.ninja/fact
get_cat_fact = SimpleHttpOperator(
    task_id='get_cat_fact',
    http_conn_id=None,  # Можно не использовать подключение
    endpoint='https://catfact.ninja/fact',  # Указываем полный URL
    method='GET',
    # response_check=log_response,
    log_response=True,  # Ответ не будет автоматически записан в логи
)

push_task >> pull_task >> get_cat_fact
