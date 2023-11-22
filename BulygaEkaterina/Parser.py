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


def analysis_data():
    resp = req.get("https://www.labquest.ru/sankt-peterburg/analizy-i-tseny/")
    urls = []

    soup = BeautifulSoup(resp.text, 'lxml')
    for item in soup.find_all('a', 'nav-link'):
        url = f"https://www.labquest.ru{item.get('href')}"
        urls.append(url)

    services = []
    for url in urls:
        resp = req.get(url)
        soup = BeautifulSoup(resp.text, 'lxml')
        for title_item, price_item in zip(soup.find_all('div', 'col-test-name'), soup.find_all('div', 'col-price')):
            title = title_item.get_text(strip=True)
            price = price_item.get_text(strip=True)
            services.append(Service(Category(soup.find('h1', 'tests-heading').get_text(strip=True)), title, price))

    data = [
        [time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), services[i].title, services[i].category.title,
         '', services[i].price, 'АО "ЛабКвест"'] for i in range(len(services))]

    df = pd.DataFrame(data,
                      columns=['Дата', 'Наименование', 'Группа', 'Код', 'Стоимость услуги', 'Наименование лаборатории'])
    df.to_csv('Анализы.csv')


def address_data():
    resp = req.get("https://www.labquest.ru/sankt-peterburg/adresa-i-vremya-raboty/")

    addresses = []
    soup = BeautifulSoup(resp.text, 'lxml')
    for title_item, worktime_item in zip(soup.find_all('div', 'title-block'), soup.find_all('div', 'address-worktime')):
        title = title_item.get_text(strip=True).split(', ')
        worktime = [item.strip() for item in worktime_item.get_text().split('\n') if item.strip()]
        addresses.append(Address(title[0], ', '.join(title[1:]), worktime,
                                 soup.find('span', 'msk_call_phone_1').get_text(strip=True).replace(' ', ''), ''))

    data = [[addresses[i].city, addresses[i].address, addresses[i].worktime, addresses[i].phone, addresses[i].metro]
            for i in range(len(addresses))]
    df = pd.DataFrame(data=data, columns=['Город', 'Адрес', 'Часы работы', 'Контактный телефон', 'Станция метро'])
    df.to_csv('Адреса.csv')


if __name__ == '__main__':
    analysis_data()
    address_data()
