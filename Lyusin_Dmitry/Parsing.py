from bs4 import BeautifulSoup
import requests as req
import pandas as pd
import time
from database import db


class Address:
    def __init__(self, city, address, worktime, phone, metro, laboratory='INVITRO'):
        self.city = city
        self.address = address
        self.worktime = worktime_to_str(worktime)
        self.phone = phone
        self.metro = metro
        self.laboratory = laboratory


class Category:
    def __init__(self, title):
        self.title = title


class Service:
    def __init__(self, category, title, price, code='', laboratory='INVITRO'):
        self.category = category
        self.title = title
        self.price = str(clear_price(price))
        self.laboratory = laboratory
        self.code = str(code)
        self.date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))


def worktime_to_str(worktime):
    return ', '.join([elem for elem in worktime])


def clear_price(price):
    if not price.isdigit():
        price = ''.join([i for i in price if i.isdigit()])
    if price.isdigit():
        price = int(price)
    return price


def address_data():
    resp = req.get("https://www.invitro.ru/offices/piter/")

    addresses = []
    soup = BeautifulSoup(resp.text, 'lxml')
    for title_item, metro_item in zip(soup.find_all('div', 'offices_card__address offices_card__address--title'),
                                      soup.find_all('div', 't_14 offices_card__location')):
        title = title_item.get_text(strip=True).split(', ')
        metro = metro_item.get_text(strip=True).split(', ')
        metro = metro[-1].split('м. ')[-1]
        if '.' in metro or 'литера' in metro:
            metro = ''
        if 'г. Санкт-Петербург' in title:
            addresses.append(Address(title[0], ', '.join(title[1:]), '',
                                     soup.find('a', 'invitro_header-phone__link').get_text(strip=True).replace(' ', ''),
                                     metro))

    return addresses
    # data = [[addresses[i].city, addresses[i].address, addresses[i].worktime, addresses[i].phone, addresses[i].metro]
    #        for i in range(len(addresses))]
    # df = pd.DataFrame(data=data, columns=['Город', 'Адрес', 'Часы работы', 'Контактный телефон', 'Станция метро'])
    # df.to_csv('Адреса.csv')


def analysis_data():
    resp = req.get("https://www.invitro.ru/analizes/for-doctors/piter/")
    urls = []

    soup = BeautifulSoup(resp.text, 'lxml')
    for item in soup.find_all('a', 'side-bar-second__link side-bar__link--third'):
        url = f"https://www.invitro.ru/analizes/for-doctors/piter/{item.get('href')}"
        urls.append(url)

    services = []
    for url in urls:
        resp = req.get(url)
        soup = BeautifulSoup(resp.text, 'lxml')
        for title_item, price_item in zip(soup.find_all('div', 'analyzes-item__title'),
                                          soup.find_all('div', 'analyzes-item__total--sum')):
            title = title_item.get_text(strip=True)
            price = price_item.get_text(strip=True)
            services.append(Service(Category(''), title, price))

    return services

    # data = [
    #    [time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), services[i].title, services[i].category.title,
    #     '', services[i].price, 'INVITRO'] for i in range(len(services))]

    # df = pd.DataFrame(data,
    #                  columns=['Дата', 'Наименование', 'Группа', 'Код', 'Стоимость услуги', 'Наименование лаборатории'])
    # df.to_csv('Анализы.csv')


async def add_to_db(obj, table_name):
    if isinstance(obj, Service):
        # Рассмотрим добавление данных в таблицу анализов схемы dds
        await db._insert(
            table=table_name,  # Тут название таблицы (analyzes/addresses)
            schema='stg',  # Указываем схему
            Код=obj.code,  # Здесь код анализа
            Группа=obj.category.title,  # Группа, к которой относится наши анализы
            Наименование=obj.title,  # Наименование нашего анализа
            Стоимость_услуги=obj.price,  # Стоимость услуги
            Наименование_лаборатории=obj.laboratory,  # Наименование лаборатории
            Дата=obj.date,  # Дата сбора анализа
        )
    elif isinstance(obj, Address):
        await db._insert(
            table=table_name,  # Тут название таблицы (analyzes/addresses)
            schema='stg',  # Указываем схему
            Город=obj.city,
            Адрес=obj.address,
            Контактные_номера=obj.phone,
            Часы_работы=obj.worktime,
            Станция_метро=obj.metro,
            Наименование_клиники=obj.laboratory
        )


async def parse():
    services = analysis_data()
    addresses = address_data()

    for service in services:
        await add_to_db(service, 'analyzes')

    for address in addresses:
        await add_to_db(address, 'addresses')
