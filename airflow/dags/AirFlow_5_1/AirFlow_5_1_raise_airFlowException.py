from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python import PythonOperator


# TODO: Этот Dag не заработал на локальной машине по причине ошибки связанной с верификацией сертификата на локальной машине
# Я не смог ее решить, потому что прозодил курс на рабочем компе а на нем стоит касперский который мешает, отключить я его не пытался


def on_success_callback(context):
    send_message = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id="telegram_id_555995750",
        chat_id="-4537471040",
        text="It's work",
        dag=dag,  # Получаем dag из контекста
    )
    return send_message.execute(context=context)


def on_failure_callback(context):
    send_message = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id="telegram_id_555995750",
        chat_id="-4537471040",
        text="It isn't work",
        dag=dag,  # Получаем dag из контекста
    )
    return send_message.execute(context=context)

# Такая реализация ниже для эмитации ошибки
def raise_error():
    raise AirflowException("This is a simulated error!")


with DAG(
    '555995750',
    start_date=datetime(2024, 10, 6),
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    schedule_interval='@daily',
    tags=["555995750"],
) as dag:

    # Задача, которая вызывает ошибку
    raise_error_task = PythonOperator(
        task_id='raise_error_task', python_callable=raise_error
    )

    raise_error_task
