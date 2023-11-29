from bs4 import BeautifulSoup
import requests as req
import pandas as pd
import time


class Address:
    def __init__(self, city, address, worktime, phone, metro):
        self.city = city
        self.address = address
        self.worktime = worktime_to_str(worktime)
        self.phone = phone
        self.metro = metro


class Category:
    def __init__(self, title):
        self.title = title


class Service:
    def __init__(self, category, title, price):
        self.category = category
        self.title = title
        self.price = clear_price(price)


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
    for title_item, metro_item in zip(soup.find_all('div', 'offices_card__address offices_card__address--title'), soup.find_all('div', 't_14 offices_card__location')):
        title = title_item.get_text(strip=True).split(', ')
        metro = metro_item.get_text(strip=True).split(', ')
        metro = metro[-1].split('м. ')[-1]
        if '.' in metro or 'литера' in metro:
            metro = ''
        if 'г. Санкт-Петербург' in title:
           addresses.append(Address(title[0], ', '.join(title[1:]), '',
                                    soup.find('a', 'invitro_header-phone__link').get_text(strip=True).replace(' ', ''), metro))

    data = [[addresses[i].city, addresses[i].address, addresses[i].worktime, addresses[i].phone, addresses[i].metro]
            for i in range(len(addresses))]
    df = pd.DataFrame(data=data, columns=['Город', 'Адрес', 'Часы работы', 'Контактный телефон', 'Станция метро'])
    df.to_csv('Адреса.csv')

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
        for title_item, price_item in zip(soup.find_all('div', 'analyzes-item__title'), soup.find_all('div', 'analyzes-item__total--sum')):
            title = title_item.get_text(strip=True)
            price = price_item.get_text(strip=True)
            services.append(Service(Category(soup.find('div', 'title-block title-block--img').get_text(strip=True)), title, price))

    data = [
        [time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), services[i].title, services[i].category.title,
         '', services[i].price, 'INVITRO'] for i in range(len(services))]

    df = pd.DataFrame(data,
                      columns=['Дата', 'Наименование', 'Группа', 'Код', 'Стоимость услуги', 'Наименование лаборатории'])
    df.to_csv('Анализы.csv')

if __name__ == '__main__':
    address_data()
    analysis_data()

