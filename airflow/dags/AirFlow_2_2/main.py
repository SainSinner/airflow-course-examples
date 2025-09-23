import pandas as pd
import sqlite3
from send_email import _send_email
from log_pas_email import username, password
CON = sqlite3.connect("/AirFlow_2_2/example.db")
# CON = sqlite3.connect('../example.db')
HOST = 'smtp.gmail.com'
TO = 'airflo2000@gmail.com'
FROM = 'g.cvyat@gmail.com'


# Выгрузка данных с сайта
def extract_data(url):
    return pd.read_csv(url)


# Группировка данных
def transform_data(data, group, agreg):
    return data.groupby(group).agg(agreg).reset_index()


# Загрузка в базу данных
# Для тех кто не работал с pandas+sqlite
# data_frame.to_sql(...) автоматически создаст sqlite базу данных и таблицу
def load_data(data, table_name, conn=CON):
    data["insert_time"] = pd.to_datetime("now")
    data.to_sql(table_name, conn, if_exists='replace', index=False)


# Отправка данных на почту
def send_email(data, username, password, host, port, to, From) -> None:
    """ Send to email
    """
    _send_email(data=data, username=username, password=password, host=host, port=port, to=to, From=From)


# Очистка таблицы
def truncate_table(table_name, conn=CON):
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM {table_name}")
    conn.commit()


# Чтение данных из таблицы
def read_data(table_name, conn=CON):
    return pd.read_sql(f'SELECT * FROM "{table_name}"', conn)


if __name__ == '__main__':
    # Выгрузка
    data = extract_data(
        "https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data/data.csv")

    # Трансформация данных
    data = transform_data(data,
                          group=['A', 'B', 'C'],
                          agreg={"D": "sum"})
    # Очистка таблицы перед загрузкой новых данных (если нужно)
    # truncate_table("test_table")

    # Загрузка в таблицу
    load_data(data, "test_table")

    # Отправка Email
    send_email(data=data, username=username, password=password, host=HOST, port=587, to=TO, From=FROM)

    # Чтение данных из таблицы
    table_data = read_data("test_table")
    print(table_data)
