from airflow.models.baseoperator import BaseOperator
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
import pandas as pd


class FileSQLiteTransferHook(SqliteHook):

    def get_pandas_df(self, url_or_path, path_local):
        """ Ваш код который читает данные из файла
        """
        # Ваш код который читает данные из файла
        data = pd.read_csv(url_or_path)
        data.to_csv(path_or_buf=path_local + 'get_pandas_df.csv', index=False)
        return data

    def insert_df_to_db(self, data, table_name):
        """ Данный метод вставляет данные в БД
            self.get_conn() это готовый метод SqliteHook для создания подключения
        """
        # data = self.get_pandas_df(data)
        data.to_sql(table_name, con=self.get_conn(), if_exists='replace', index=False)

    def read_data(self, table_name):
        return print(pd.read_sql(f'SELECT * FROM "{table_name}"', con=self.get_conn()))


class FileSQLiteTransferOperator(BaseOperator):

    def __init__(self, path, path_local, table_name, **kwargs):
        super().__init__(**kwargs)
        self.hook = None
        self.path = path  # Путь до файла
        self.path_local = path_local  # Путь до личной папки
        self.table_name = table_name  # Название таблицы в BD

    def execute(self, context):
        # Создание объекта хука
        self.hook = FileSQLiteTransferHook()

        # Ваш код вызовите метод который
        # читает данные и затем записывает данные в БД
        data = self.hook.get_pandas_df(url_or_path=self.path, path_local=self.path_local)
        self.hook.insert_df_to_db(data=data, table_name=self.table_name)
        self.hook.read_data(table_name=self.table_name)


# Запуск вашего Оператора

dag = DAG('555995750', schedule_interval=timedelta(days=1), start_date=days_ago(1),
          tags=["555995750"], )

t1 = FileSQLiteTransferOperator(
    task_id='transfer_data',
    path='https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data_new/2021-01-04.csv',
    path_local='/usr/local/airflow/dags/sandbox/555995750/',
    table_name='test_table_555995750',
    dag=dag)
