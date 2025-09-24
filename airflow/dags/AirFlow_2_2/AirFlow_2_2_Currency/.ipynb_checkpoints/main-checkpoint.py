# Импорт необходимых библиотек
import pandas as pd
import requests
from io import StringIO
import os

URL_excangeRate = 'https://raw.githubusercontent.com/datanlnja/airflow_course/main/excangerate/'
URL_data = 'https://raw.githubusercontent.com/datanlnja/airflow_course/main/data/'
FILE_PATH = 'excangeRateAndData.csv'
DATE_LIST = ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04"]


# Напишите функцию которая скачивает данные с гитхаба
# Функция принимает на вход дату
# Функция должна вернуть список словарей или датафрейм, можно(лучше) использовать можно использоват pandas.read_csv()

# С помощью этой функции вы считаете данные по 2 ссылкам
# ссылка на гитхаб с курсами валют - https://github.com/datanlnja/airflow_course/tree/main/excangerate
# ссылка на гитхаб с данными о продажах - https://github.com/datanlnja/airflow_course/tree/main/data

def extract_data(url, date):
    # Отправляем GET-запрос по URL
    url = url + date + '.csv'
    response = requests.get(url, verify=False)
    # Убедимся, что запрос был успешным
    if response.status_code == 200:
        # Преобразуем содержимое ответа в строку и читаем его в DataFrame
        csv_data = StringIO(response.text)
        update = pd.read_csv(csv_data)
        return update

    else:
        print(f"Ошибка загрузки файла, статус код: {response.status_code}")


# Напишите функцию которая вставляет данные в файл (не перезаписывает.. а добавляет новые!)
def insert_to_file(udate):
    if os.path.exists(FILE_PATH):
        udate.to_csv(FILE_PATH, mode='a', header=False, index=False, encoding='utf-8')
    else:
        udate.to_csv(FILE_PATH, index=False, encoding='utf-8')


# Напишите функцию которая обюъединит данные по ключу или паре ключей
# На выходе возвращает данные, рекомендую использовать pandas.DataFrame

def merge_data(data, currency, left_on="currency", right_on="currency_from"):
    currency["currency_from"] = currency["currency_from"].str.upper()
    currency["currency_to"] = currency["currency_to"].str.upper()
    merged_df = pd.merge(data, currency, left_on=left_on, right_on=right_on, how='left')
    merged_df = merged_df[['date_x', 'currency_from', 'currency_to', 'amount', 'value']]
    merged_df = merged_df.rename(columns={'currency_from': 'code', 'currency_to': 'base', 'date_x': 'date'})
    return merged_df


# Запустите ваш код в функции main

# Напишите генерацию дат, так чтобы у вас получился список
# 2021-01-02, 2021-01-03 ... etc
# Нужны даты с 2021-01-01 по 2021-01-04

def main(date):
    # Выгружаем данные по валютам и из источника
    currency = extract_data(URL_excangeRate, date)
    data = extract_data(URL_data, date)

    # Объедините данные в 1 таблицу
    mg_data = merge_data(data=data, currency=currency)

    # Вставляем данные в файл
    insert_to_file(mg_data)


# Удаляем файл, чтобы получать корректный ответ
if os.path.exists(FILE_PATH):
    os.remove(FILE_PATH)

# Пройдемся по списку дат и выполним скрипт
for date in DATE_LIST:
    main(date=date)

df = pd.read_csv(FILE_PATH)
df['amount*value'] = df['amount'] * df['value']
sum_amount_value = df['amount*value'].sum()
print("Ответ: " + sum_amount_value)
