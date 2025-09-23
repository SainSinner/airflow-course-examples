from airflow import DAG
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
# Кастомный плагин необходимо создавать и указывать до него путь
from custom_plugin.operators.custom_operator import HelloOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator


# TODO: Этот Dag не заработал на локальной машине по причине ошибки связанной с верификацией сертификата на локальной машине
# Я не смог ее решить, потому что прозодил курс на рабочем компе а на нем стоит касперский который мешает, отключить я его не пытался


def on_success_callback(context):
    send_message = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id="telegram_id_555995750",
        chat_id="-4537471040",
        text="Hello from Airflow!8912",
        dag=dag,  # Получаем dag из контекста
    )
    return send_message.execute(context=context)


# Создаем объект класса DAG
dag = DAG(
    '555995750',
    start_date=datetime(2024, 10, 6),
    on_success_callback=on_success_callback,
    schedule_interval='@daily',
    tags=["555995750"]
)

hello_task = HelloOperator(task_id='sample-task', name='foo_bar', dag=dag)


