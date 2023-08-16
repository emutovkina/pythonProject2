import requests
from bs4 import BeautifulSoup
import sqlite3

url = "https://api.hh.ru/vacancies"
url_params = {
    "text": "middle python",
    "search_field": "name",
    "per_page": "30",
    "page": "2"
}
result = requests.get(url, params=url_params)

vacancies = result.json().get('items')

for vacancy in vacancies:
    print(vacancy['name'], vacancy['alternate_url'], vacancy['employer']['name'])
