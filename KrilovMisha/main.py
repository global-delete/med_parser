import json
import requests
import datetime
import csv
import asyncio  

from bs4 import BeautifulSoup
from typing import List
from KrilovMisha.database import db

fieldnames_analysis: List = ['Код', 'Группа', 'Наименование', 'Стоимость_услуги', 'Наименование_лаборатории', 'Дата']
fieldnames_centers: List = ['Город', 'Адрес', 'Контактные_номера', 'Часы_работы', 'Станция_метро', 'Наименование_клиники']

class Parser:
    __url: str = "https://www.cmd-online.ru"
    __path_data_analysis = r".\KrilovMisha\data\analysis.csv"
    __path_data_centers = r".\KrilovMisha\data\centers.csv"
    __city = 'sankt-peterburg'

    __headers: dict = {
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    }

    def __init__(self) -> None:
        self.session: requests.Session = requests.Session()
        self.session.headers: dict = self.__headers

        self.data: dict = {}
        self.date: datetime = datetime.datetime.now()

    def get_soup(self, url: str) -> BeautifulSoup:            
        return BeautifulSoup(self.session.get(url, timeout=1000).text, "lxml")

    def parse_page(self, url: str) -> List[dict]:
        page_data = []

        soup = self.get_soup(url)
        for section in soup.find_all('section', class_='analyze-section'):
            for analyze in section.find_all("article", class_="analyze-item"):
                data = {}

                data['Группа'] = section.find('h2', class_='analyze-section__title').text.strip()

                data["Наименование"] = analyze.find(
                    "div", class_="analyze-item__title"
                ).text.strip()

                data["Код"] = analyze.find_all(
                    "dl", class_="analyze-item__spec"
                )[0].text.replace("Код:", "").strip()

                data["Стоимость_услуги"] = analyze.find(
                    "div", class_="analyze-item__price"
                ).text.replace("Цена:", "").strip()

                data['Наименование_лаборатории'] = "CMD"
                data['Дата'] = "{}.{}.{}".format(self.date.day, self.date.month, self.date.year)

                page_data.append(data)

        return page_data

    def parse_city_analysis(self, city) -> List:
        analysis = []

        soup = self.get_soup(f"{self.__url}/analizy-i-tseny/katalog-analizov/{city}/?all_group=&set_filter=y")

        count_of_page = int(
            soup.find_all("li", class_="pagination__item")[-1].text.strip()
        )

        for page in range(1, count_of_page + 1):
            analysis += self.parse_page(
                f"{self.__url}/analizy-i-tseny/katalog-analizov/{city}/?all_group=&set_filter=y&PAGEN_1={page}"
            )

        return analysis

    def parse_city_centers(self, city) -> dict:
        soup = self.get_soup(f"{self.__url}/patsientam/gde-sdat-analizy/{city}/")

        return [self.parse_center(self.__url+i.find('a').get('href')) for i in soup.find_all('li', class_='office-list__item visible-office')]

    def parse_center(self, url) -> dict:
        soup = self.get_soup(url)
        
        return {
            'Город': soup.find('li', class_='header__links-link header__links-link--city').text.strip(),
            'Адрес': soup.find('address', class_='med-office__address').text.strip(),
            'Контактные_номера': '; '.join([i.find('a').text.strip() for i in soup.find_all('ul', class_='phones')]),
            'Часы_работы': '; '.join(f'{i.get("data-day")}: {i.text.strip()}' for i in soup.find('table', class_='custom-table').find('tbody').find('tr').find_all('td') if i.get('data-day')),
            'Станция_метро': '',
            'Наименование_клиники': '',
        }

    def parse(self) -> List:

        self.data[self.__city] = {
            'Анализы': self.parse_city_analysis(self.__city),
            'Адреса офисов': self.parse_city_centers(self.__city)
        }

        # Сохраняем данные адресов
        with open(self.__path_data_centers, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames_centers)
            writer.writeheader()
            for city in self.data:
                writer.writerows(self.data[city]['Адреса офисов'])

        # Сохраняем данные анализов
        with open(self.__path_data_analysis, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames_analysis)
            writer.writeheader()
            for city in self.data:
                writer.writerows(self.data[city]['Анализы'])

        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self.save_to_db())

        print('Done!')

    async def save_to_db(self) -> None:
        for i in self.data[self.__city]['Анализы']:
            await db._insert(table='analyzes', schema='stg', **i)

        for i in self.data[self.__city]['Адреса офисов']:
            await db._insert(table='addresses', schema='stg', **i)

Parser().parse()
