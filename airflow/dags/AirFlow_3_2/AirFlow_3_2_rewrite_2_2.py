import pandas as pd
import sqlite3
import os

from send_email import _send_email
from log_pas_email import username, password

CON = sqlite3.connect("example.db")

# Файлы записываются на диск, а не передаются в потоке исполнения
# это особенность работы Airflow которую мы обсудим позднее
# **context это необязательный аргумент, но мы будем его использовать далее
# это мощный инструмент который позволит нам писать идемпотентные скрипты

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator

HOST = 'smtp.gmail.com'
TO = 'airflo2000@gmail.com'
FROM = 'g.cvyat@gmail.com'
URL_excangeRate = 'https://raw.githubusercontent.com/datanlnja/airflow_course/main/excangerate/'
URL_data = 'https://raw.githubusercontent.com/datanlnja/airflow_course/main/data/'
# FILE_PATH = 'excangeRateAndData.csv'
DATE = "2021-01-01"


# Выгрузка данных с сайта
def extract_data(url, date, tmp_file, **context):
    url = url + date + '.csv'
    pd.read_csv(url).to_csv(tmp_file)


# Группировка данных
def merge_data(currency_file, tmp_file, tmp_agg_file, left_on="currency", right_on="currency_from", **context):
    data_currency = pd.read_csv(currency_file)
    data_currency["currency_from"] = data_currency["currency_from"].str.upper()
    data_currency["currency_to"] = data_currency["currency_to"].str.upper()
    data = pd.read_csv(tmp_file)
    merged_df = pd.merge(data, data_currency, left_on=left_on, right_on=right_on, how='left')
    merged_df = merged_df[['date_x', 'currency_from', 'currency_to', 'amount', 'value']]
    merged_df = merged_df.rename(columns={'currency_from': 'code', 'currency_to': 'base', 'date_x': 'date'})
    merged_df.to_csv(tmp_agg_file)


# Загрузка в базу данных
# Для тех кто не работал с pandas+sqlite
# data_frame.to_sql(...) автоматически создаст sqlite базу данных и таблицу
def load_data(table_name, tmp_file, conn=CON, **context):
    data = pd.read_csv(tmp_file)
    data["insert_time"] = pd.to_datetime("now")
    data.to_sql(table_name, conn, if_exists='replace', index=False)


# Отправка данных на почту
def send_email(tmp_file, username, password, host, port, to, From) -> None:
    data = pd.read_csv(tmp_file)
    """ Send to email
    """
    _send_email(data=data, username=username, password=password, host=host, port=port, to=to, From=From)


# Создаем DAG(контейнер) в который поместим наши задачи
# Для DAG-а характерны нужно задать следующие обязательные атрибуты
# - Уникальное имя
# - Интервал запусков
# - Начальная точка запуска

dag = DAG(dag_id='AirFlow_3_2_rewrite_2_2',  # Имя нашего дага, уникальное
          default_args={'owner': 'airflow'},  # Список необязательных аргументов
          schedule_interval='@daily',  # Интервал запусков, в данном случае 1 раз в день 24:00
          start_date=days_ago(1)
          # Начальная точка запуска, это с какого момента мы бы хотели чтобы скрипт начал исполняться (далее разберем это подробнее)
          )

# Создадим задачи, которые будут запускать функции в питоне
# Выгрузка
extract_data_main = PythonOperator(
    task_id='extract_data_main',  # Имя задачи внутри Dag
    python_callable=extract_data,  # Запускаемая Python функция, описана выше

    # Чтобы передать аргументы в нашу функцию
    # их следует передавать через следующий код
    op_kwargs={
        'url': f'{URL_data}',
        'date': f'{DATE}',
        'tmp_file': '/tmp/airFlow/file.csv'},
    dag=dag
)

# Выгрузка курса валют
extract_data_currency = PythonOperator(
    task_id='extract_data_currency',  # Имя задачи внутри Dag
    python_callable=extract_data,  # Запускаемая Python функция, описана выше

    # Чтобы передать аргументы в нашу функцию
    # их следует передавать через следующий код
    op_kwargs={
        'url': f'{URL_excangeRate}',
        'date': f'{DATE}',
        'tmp_file': '/tmp/airFlow/file_currency.csv'},
    dag=dag
)

# Трансформация данных
merge_data = PythonOperator(
    task_id='merge_data',  # Имя задачи внутри Dag
    python_callable=merge_data,  # Запускаемая Python функция, описана выше
    # Чтобы передать аргументы в нашу функцию
    # их следует передавать через следующий код
    op_kwargs={
        'tmp_file': '/tmp/airFlow/file.csv',
        'tmp_agg_file': '/tmp/airFlow/file_agg.csv',
        'currency_file': '/tmp/airFlow/file_currency.csv'},
    dag=dag
)

# Загрузка в таблицу
load_data = PythonOperator(
    task_id='load_data',  # Имя задачи внутри Dag
    python_callable=load_data,  # Запускаемая Python функция, описана выше
    # Чтобы передать аргументы в нашу функцию
    # их следует передавать через следующий код
    op_kwargs={
        'tmp_file': '/tmp/airFlow/file_agg.csv',
        'table_name': 'test_table', },
    dag=dag
)

# Отправка Email
send_email = PythonOperator(
    task_id='send_email',  # Имя задачи внутри Dag
    python_callable=send_email,  # Запускаемая Python функция, описана выше
    # Чтобы передать аргументы в нашу функцию
    # их следует передавать через следующий код
    op_kwargs={
        'tmp_file': '/tmp/airFlow/file_agg.csv',
        'username': f'{username}',
        'password': f'{password}',
        'host': f'{HOST}',
        'port': 587,
        'to': f'{TO}',
        'From': f'{FROM}',
        'table_name': 'test_table', },
    dag=dag
)

# Создадим порядок выполнения задач
extract_data_main >> extract_data_currency >> merge_data >> [load_data, send_email]

# Документация
extract_data_main.doc_md = "Извлекает данные о продажах"
extract_data_currency.doc_md = "Извлекает данные о курсе валют"
merge_data.doc_md = "Мерджит данные продаж и курса валют"
load_data.doc_md = "Загружает данные в базу данных"
send_email.doc_md = "Отправляет данные на почту"
dag.doc_md = "Извлекает, трансформирует, загружает в БД, отправляет на почту"
