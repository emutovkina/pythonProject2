import sqlite3
import json
import os

pas = 'C:/Users/user/PycharmProjects/pythonProject2/HW1'

dir_list = os.listdir(pas)

for file in dir_list:
    with open(os.path.join(pas, file), "r", encoding="UTF-8", errors='ignore') as read_file:
        print('Смотрим файл ', read_file)
        data = json.load(read_file, strict=False)

# Создание базы данных
database = sqlite3.connect('hw1.db')
cursor = database.cursor()

# Создание таблицы telecom_companies
cursor.execute("""CREATE TABLE IF NOT EXISTS telecom_companies(
   name TEXT,
   full_name TEXT,
   inn TEXT,
   kpp TEXT,
   okved TEXT
   );
""")
database.commit()

# Запись информации о компаниях, занимающихся деятельностью в сфере телекоммуникаций, в базу данных
for item in data:
    try:
        if item['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'][:2] =='61':
            name = item['name']
            full_name = item['full_name']
            inn = item['inn']
            kpp = item['kpp']
            okved = item['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД']
            cursor.execute("INSERT INTO telecom_companies (name, full_name, inn, kpp, okved) VALUES (?,?,?,?,?)",(name, full_name, inn, kpp, okved))
    except KeyError:
        continue

cursor.execute("SELECT count(*) FROM telecom_companies;")
one_result = cursor.fetchone()
print('Данные успешно записаны. Количество строк: ', one_result[0])
database.close()