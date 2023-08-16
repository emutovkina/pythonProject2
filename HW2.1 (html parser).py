import requests
from bs4 import BeautifulSoup
import sqlite3
import logging

#Настраиваем логгер
logging.basicConfig(filename='C:\\Users\\user\\PycharmProjects\\pythonProject2\\my_app.log',
                    encoding='utf-8',
                    level=logging.DEBUG,
                    format="%(asctime)s %(levelname)s %(message)s")

conn = sqlite3.connect('vacancies.db')
c = conn.cursor()

# Создаем таблицу vacancies (если она не существует)
c.execute('''CREATE TABLE IF NOT EXISTS vacancies
         (company_name text, position text, job_description text, key_skills text)''')

url = 'https://hh.ru/search/vacancy'
user_agent = {'User-agent': 'Edg/90.0.818.46'}
url_params = {
    "text": "middle python",
    "search_field": "name",
    "per_page": "30",
    "page": "2"
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
                  soup_vacancy.find(attrs={"class": "bloko-tag-list"}).find_all(attrs={"class": "bloko-tag__section_text"})]
            key_skills_2 = ''
            for skill in key_skills:
                key_skills_2 += skill + ', '
            key_skills_2 = key_skills_2[:-2]
        except Exception as e:
            logging.error("Возникло исключение", exc_info=True)
            pass
        c.execute("INSERT INTO vacancies (company_name, position, job_description, key_skills) VALUES(?, ?, ?, ?)", (company_name, position, job_description, key_skills_2))

conn.commit()
c.execute("SELECT count(*) FROM vacancies;")
one_result = c.fetchone()
print('Данные успешно записаны. Количество строк: ', one_result[0])

conn.close()
