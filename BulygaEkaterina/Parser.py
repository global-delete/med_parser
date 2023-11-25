from bs4 import BeautifulSoup
import requests as req
import pandas as pd
import time


class Address:
    def __init__(self, city, address, worktime, phone, metro):
        self.city = city
        self.address = address
        self.worktime = list_to_str(worktime)
        self.phone = phone
        self.metro = metro


class Category:
    def __init__(self, title):
        self.title = title


class Service:
    def __init__(self, category, title, price, code='', laboratory='АО "ЛабКвест"'):
        self.date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        self.code = code
        self.laboratory = laboratory
        self.category = category
        self.title = title
        self.price = str_to_int(price)


def list_to_str(mylist):
    return ', '.join([elem for elem in mylist])


def str_to_int(string):
    return int(''.join([i for i in string if i.isdigit()])) if not string.isdigit() else int(string)


def url_finder(tag, classname, soup):
    urls = []
    for item in soup.find_all(tag, classname):
        current_url = f"https://www.labquest.ru{item.get('href')}"
        urls.append(current_url)
    return urls.copy()


def services_finder(urls):
    services = []
    for url in urls:
        resp = req.get(url)
        soup = BeautifulSoup(resp.text, 'lxml')
        if soup.find('a', 'tests-item-top'):
            inner_urls = url_finder('a', 'tests-item-top', soup)
            services.extend(services_finder(inner_urls))
        for title_item, price_item in zip(soup.find_all('div', 'col-test-name'), soup.find_all('div', 'col-price')):
            title = title_item.get_text(strip=True)
            price = price_item.get_text(strip=True)
            item = Service(Category(soup.find('h1', 'tests-heading').get_text(strip=True)), title, price)
            services.append(item)
    return services.copy()


def analysis_data():
    resp = req.get("https://www.labquest.ru/sankt-peterburg/analizy-i-tseny/")
    soup = BeautifulSoup(resp.text, 'lxml')
    urls = url_finder('a', 'nav-link', soup)
    services = services_finder(urls)

    data = [
        [services[i].date, services[i].title, services[i].category.title,
         services[i].code, services[i].price, services[i].laboratory] for i in range(len(services))]

    df = pd.DataFrame(data,
                      columns=['Дата', 'Наименование', 'Группа', 'Код', 'Стоимость услуги', 'Наименование лаборатории'])
    df.to_csv('Анализы.csv', encoding='utf8')


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
    df.to_csv('Адреса.csv', encoding='utf8')


if __name__ == '__main__':
    start = time.time()
    analysis_data()
    address_data()
    stop = time.time()
    print(time.strftime('%M:%S', time.localtime(stop - start)))
