import sqlite3
import urllib.request
import zipfile
import json

# Скачиваем файл по ссылке
response = urllib.request.urlopen("https://ofdata.ru/open-data/download/okved_2.json.zip")
# Открываем файл с zip архивом на чтение
data_zip = zipfile.ZipFile('C:/Users/user/Downloads/okved_2.json.zip', 'r')
# Извлекаем все файлы из zip архива
data_zip.extractall(path="C:/Users/user/PycharmProjects/pythonProject2/HW1")

# Проверяем, что файл содержит необходимые нам названия ключей
with open('C:/Users/user/PycharmProjects/pythonProject2/HW1/okved_2.json', "r", encoding="utf-8") as okved:
    data = json.load(okved)
    unique = []
    for key in data:
        for k in key:
            if k in unique:
                continue
            else:
                unique.append(k)
    print(unique)

# Устанавливаем соединение, создаем БД
connection = sqlite3.connect("hw1.db")
# Оператор SQL для создания таблицы
create_names_table = """
CREATE TABLE IF NOT EXISTS names(
code TEXT, 
parent_code TEXT, 
section TEXT, 
name TEXT, 
comment TEXT
)
"""
# Создаем курсор
cursor = connection.cursor()

# Запускаем команду создания таблицы
cursor.execute(create_names_table)

# Фиксируем изменения
connection.commit()

def get_data(cursor, table):
    sql = f"SELECT * FROM {table}"
    cursor.execute(sql)
    return cursor.fetchone()
get_data(cursor, 'names')

# Значения для параметров запроса
with open('C:/Users/user/PycharmProjects/pythonProject2/HW1/okved_2.json', "r", encoding="utf-8") as okved:
    rows = json.load(okved)

# Подготавливаем запрос с именованными параметрами
insert_several_rows_parameters = """
INSERT INTO names (code, parent_code, section, name, comment)
VALUES (:code, :parent_code, :section, :name, :comment)
"""
# Запускаем команду вставки нескольких элементов данных
cursor.executemany(insert_several_rows_parameters, rows)

# Фиксируем изменения
connection.commit()

get_data(cursor, 'names')

