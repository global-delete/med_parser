import json
import requests
import datetime
import csv

from bs4 import BeautifulSoup

DEBBUGING = False

fieldnames_analysis: list = ['Город', 'Код', 'Наименование', 'Срок', 'Стоимость услуги', 'Наименование лаборатории', 'Дата']
fieldnames_centers: list = ['Город', 'Адреса офисов']

class Parser:
    __url: str = "https://www.cmd-online.ru"
    __path_cities: str = "cities.json"
    __path_data_analysis = r".\data\analysis.csv"
    __path_data_centers = r".\data\centers.csv"

    __headers: dict = {
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    }

    def __init__(self) -> None:
        self.session: requests.Session = requests.Session()
        self.session.headers: dict = self.__headers

        self.data: dict = {}
        self.cities: list = self.get_cities()
        self.date: datetime = datetime.datetime.now()

    def get_cities(self) -> list:
        with open(self.__path_cities) as file:
            return json.load(file)

    def get_soup(self, url: str) -> BeautifulSoup:
        if DEBBUGING:
            print(url)
            
        return BeautifulSoup(self.session.get(url).text, "lxml")

    def parse_page(self, url: str) -> list[dict]:
        page_data = []

        soup = self.get_soup(url)

        for analyze in soup.find_all("article", class_="analyze-item"):
            data = {}

            data['Город'] = soup.find(
                'li', class_='header__links-link header__links-link--city'
            ).text.strip()

            data["Наименование"] = analyze.find(
                "div", class_="analyze-item__title"
            ).text.strip()

            data["Код"] = analyze.find_all(
                "dl", class_="analyze-item__spec"
            )[0].text.replace("Код:", "").strip()

            data["Срок"] = analyze.find_all(
                "dl", class_="analyze-item__spec"
            )[1].text.replace("Срок:", "").strip()

            data["Стоимость услуги"] = analyze.find(
                "div", class_="analyze-item__price"
            ).text.replace("Цена:", "").strip()

            data['Наименование лаборатории'] = "CMD"
            data['Дата'] = "{}.{}.{}".format(self.date.day, self.date.month, self.date.year)

            page_data.append(data)

        return page_data

    def parse_city_analysis(self, city) -> list:
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

    def parse_city_centers(self, city) -> list:
        soup = self.get_soup(f"{self.__url}/patsientam/gde-sdat-analizy/{city}/")

        return {
            "Город": soup.find('li', class_='header__links-link header__links-link--city').text.strip(),
            "Адреса офисов": [i.text.strip() for i in soup.find_all('div', class_='office-item__address')]
        }

    def parse(self) -> list:
        for city in self.cities:
            self.data[city] = {
                'Анализы': self.parse_city_analysis(city),
                'Адреса офисов': [self.parse_city_centers(city)]
            }

        # Сохраняем данные адресов
        with open(self.__path_data_centers, 'w', encoding='cp1251', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames_centers)
            writer.writeheader()
            for city in self.data:
                writer.writerows(self.data[city]['Адреса офисов'])

        # Сохраняем данные анализов
        with open(self.__path_data_analysis, 'w', encoding='cp1251', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames_analysis)
            writer.writeheader()
            for city in self.data:
                writer.writerows(self.data[city]['Анализы'])


Parser().parse()

