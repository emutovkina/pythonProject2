import requests
import zipfile
import pandas as pd
import json
from bs4 import BeautifulSoup
import re
import sqlite3
import logging
from collections import Counter
import urllib.request
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

pas = 'C:/Users/user/PycharmProjects/pythonProject2/HW1'
url = "https://ofdata.ru/opendata/download/egrul.json.zip"

def download_egrul_json(url):
    response = urllib.request.urlopen(url)

def extract_egrul_json(pas):
    data_zip = zipfile.ZipFile('C:/Users/user/Downloads/egrul.json.zip', 'r')
    data_zip.extractall(path=pas)

def companies_names(pas):
    list_companies = []
    dir_list = os.listdir(pas)
    for file in dir_list:
        with open(os.path.join(pas, file), "r", encoding="UTF-8", errors='ignore') as read_file:
            data = json.load(read_file, strict=False)
            for item in data:
                try:
                    if item['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'][:2] == '61':
                        name = item['name']
                        list_companies.append(name)
                except KeyError:
                    continue
    return list_companies

def search_vacancies():
    logging.basicConfig(filename='C:\\Users\\user\\PycharmProjects\\pythonProject2\\my_app.log',
                        encoding='utf-8',
                        level=logging.DEBUG,
                        format="%(asctime)s %(levelname)s %(message)s")

    conn = sqlite3.connect('vacancies.db')
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS vacancies
             (company_name text, position text, job_description text, key_skills text)''')

    url = 'https://hh.ru/search/vacancy'
    user_agent = {'User-agent': 'Edg/90.0.818.46'}
    url_params = {
        "text": "middle python",
        "search_field": "name",
        "per_page": "20",
        "page": "3"
    }
    result = requests.get(url, headers=user_agent, params=url_params)
    soup = BeautifulSoup(result.content.decode(), 'html.parser')
    vacancies = soup.find_all('a', attrs={'data-qa': 'serp-item__title'})

    for vacancy in vacancies:
        url_detail = vacancy.get('href')
        result_vacancy = requests.get(url_detail, headers=user_agent)
        result_vacancy.content.decode()
        soup_vacancy = BeautifulSoup(result_vacancy.content.decode(), "html.parser")
        try:
            company_name = soup_vacancy.find('a', attrs={'data-qa': 'vacancy-company-name'}).text
            position = soup_vacancy.find('h1', attrs={'data-qa': 'vacancy-title'}).text
            job_description = soup_vacancy.find('div', attrs={'data-qa': 'vacancy-description'}).text
            key_skills = [tag.text.replace("\xa0", " ") for tag in
                          soup_vacancy.find(attrs={"class": "bloko-tag-list"}).find_all(
                              attrs={"class": "bloko-tag__section_text"})]
            key_skills_2 = ''
            for skill in key_skills:
                key_skills_2 += skill + ', '
            key_skills_2 = key_skills_2[:-2]
        except Exception as e:
            logging.error("Возникло исключение", exc_info=True)
            pass
        c.execute("INSERT INTO vacancies (company_name, position, job_description, key_skills) VALUES(?, ?, ?, ?)",
                  (company_name, position, job_description, key_skills_2))

    conn.commit()
    c.execute("SELECT count(*) FROM vacancies;")
    one_result = c.fetchone()
    print('Данные успешно записаны. Количество строк: ', one_result[0])
    conn.close()

def get_top_skills(list_companies):
    try:
        conn = sqlite3.connect('vacancies.db')
        c = conn.cursor()

        df_vacancies = pd.read_sql_query("SELECT * FROM vacancies", conn)

        # Очищаем от правовых форм и запятых (в вакансиях)
        repl_list = '|'.join(['ООО ', 'АО ', 'ПАО '])
        df_vacancies['company_name'] = \
        df_vacancies['company_name'].str.replace(repl_list, '', regex=True, case=False).str.split(r',').str[0]
        list_companies = str(list_companies).split(r'"').str[1]

        # Ищем совпадения между названиями компаний
        df_vacancies = df_vacancies.assign(exst=df_vacancies['company_name'].isin(list_companies).astype(int))
        df_filtred_vac = df_vacancies.loc[df_vacancies['exst'] == 1]

        # Считаем топ и записываем
        list_keys = list(df_filtred_vac['key_skills'])
        top_key_skills = dict(Counter(', '.join(list_keys).split(', ')))
        top_key_skills = dict(sorted(top_key_skills.items(), key=lambda x: x[1], reverse=True))

        with open('C:/Users/user/PycharmProjects/pythonProject2/HW1/top.txt', "w", encoding="utf-8") as file:
            file.write("\n".join(top_key_skills))

    except Exception as err:
        logging.error("Возникло исключение", exc_info=True)
        pass

with DAG(dag_id="data_pipeline", start_date=datetime(2023, 8, 15), schedule_interval="@daily") as dag:
    download_egrul_json_task = PythonOperator(task_id="download_egrul_json_task", python_callable=download_egrul_json)
    extract_egrul_json_task = PythonOperator(task_id="extract_egrul_json_task", python_callable=extract_egrul_json)
    companies_names_task = PythonOperator(task_id="companies_names_task", python_callable=companies_names)
    search_vacancies_task = PythonOperator(task_id="search_vacancies_task", python_callable=search_vacancies)
    get_top_skills_task = PythonOperator(task_id="get_top_skills_task", python_callable=get_top_skills)

    download_egrul_json_task >> extract_egrul_json_task
    extract_egrul_json_task >> companies_names_task
    companies_names_task >> search_vacancies_task
    search_vacancies_task >> get_top_skills_task