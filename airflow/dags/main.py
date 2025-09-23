import pandas as pd

# Чтение CSV-файла
df = pd.read_csv('M03.01 PP Calculator for clean--2024.10.30--18.03.csv', delimiter=';')

# # Преобразование всех столбцов в строки
# df = df.astype(str)
# print(df)
#
# # Сохранение результата в новый CSV
# df.to_csv('your_file_converted.csv', index=False)

# Добавление кавычек к каждому значению
df = df.apply(lambda col: col.map(lambda x: f'"{x}"'))

# Сохранение результата в новый CSV, не экранируя заголовки
df.to_csv('your_file_quoted.csv', index=False, quoting=3, escapechar='\\')