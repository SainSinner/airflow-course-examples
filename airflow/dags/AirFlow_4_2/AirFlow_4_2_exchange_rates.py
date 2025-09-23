from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import os

import requests  # Модуль для HTTP запросов


def load_data(ds, tmp_file, tmp_file_number, tmp_file_type, **context):
    # Здесь вам необходимо получить доступ к параметрам Connections
    host = BaseHook.get_connection("exchange_rate_1").host
    access_key = BaseHook.get_connection("exchange_rate_1").password

    params = {
        "access_key": access_key,
        "start_date": ds,
        "end_date": ds,
        "currencies": "GBP",
    }

    # Делаем запрос к сервису
    response = requests.get(host, params=params)

    # Здесь вам нужно написать код который складывает данные в файл
    # Далее вам нужно выбрать самое большое значение USDGBP это будет ответом
    if response.status_code == 200:
        data = response.json()
        # забираем значение из тэга quotes ответного json'а
        quotes = data.get("quotes", {})

        # читаем в data frame данные из quotes
        df = pd.DataFrame(quotes)

        # меняем местами индексы с заголовками (транспонируем)
        df = df.T

        # Собираем путь для файла
        file_path = tmp_file + tmp_file_number + tmp_file_type

        # Если файла не существует то создаем его с заголовками, если существует то просто добавляем в него свежие данные
        if not os.path.exists(file_path):
            df.to_csv(file_path, mode='w', header=True, index=True)
        else:
            df.to_csv(file_path, mode='a', header=False, index=True)

        # читаем csv в data frame
        df = pd.read_csv(file_path, index_col=0)

        # определяем макимальное значение для определенного столбца
        max_usdgbp = df['USDGBP'].max()

        # определяем индекс максимального значение для определенного столбца
        max_usdgbp_data = df['USDGBP'].idxmax()

        # выводим актуальное содержимое файла
        print(df)

        # выводим ответ
        print(f"Максимальное значение на дату {ds} для USDGBP составляло {max_usdgbp} и было {max_usdgbp_data}")


# Определяем аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,  # Устанавливаем зависимость от прошлого выполнения
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG('AirFlow_4_2_exchange_rates',
         default_args=default_args,
         schedule_interval='@daily',
         start_date=datetime(2024, 1, 1),
         end_date=datetime(2024, 1, 4),
         catchup=True  # Учитываем все промежутки времени в периоде запуска
         ) as dag:
    # Создадим оператор для исполнения python функции
    t1 = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_kwargs={
            'ds': '{{ds}}',
            'tmp_file': '/tmp/airFlow/AirFlow_4_2_exchange_rates_',
            'tmp_file_number': '0',
            'tmp_file_type': '.csv'
        }
    )
