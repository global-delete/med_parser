import time

import pandas
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from database import db

categories = [
    "check-up-obsledovanie-organizma/",
    "kompleksnye-obsledovaniya/",
    "analizy-krovi/",
    "dlya_vegetariantsev_I_veganov/",
    "biokhimiya-krovi/",
    "gormony/",
    "vitaminy_i_mineraly/",
    "gospitalizatsiya_operatsiya/",
    "infektsii/",
    "kosti/",
    "onkomarkery/",
    "lishniy_ves/",
    "pcr/",
    "podzheludochnaya-zheleza/",
    "pechen-i-zhelchevyvodyashchie-puti/",
    "krov-i-sistema-krovetvoreniya/",
    "allergiya-i-immunitet/",
    "analizy-mochi-i-kala/",
    "shchitovidnaya-zheleza/",
    "pochki/",
    "bronkhi-i-lyegkie/",
    "zabolevaniya-vyzvannye-gelmintami-i-prosteyshimi/",
    "krov-na-sakhar/",
    "svertyvaemost-krovi/",
    "serdtse-i-sosudy/",
    "beremennym/",
    "sportsmenam/",
    "avtorskie_kompleksy/",
    "gruppa-krovi/",
    "genetika/",
]
cat_names = [
    "Check-up обследование организма",
    "Комплекс анализов",
    "Клинический (общий) анализ крови",
    "Вегетарианцам и веганам",
    "Биохимия крови",
    "Гормоны",
    "Витамины и минералы",
    "Госпитализация, операция",
    "Инфекции",
    "Кости, мышцы, суставы",
    "Онкомаркеры",
    "Лишний вес",
    "ПЦР-исследования",
    "ЖКТ и поджелудочная железа",
    "Печень и желчевыводящие пути",
    "Анемии",
    "Аллергия и иммунитет",
    "Анализы мочи и кала",
    "Щитовидная железа",
    "Почки",
    "Бронхи и легкие",
    "Паразиты",
    "Кровь на сахар",
    "Свертываемость крови",
    "Сердце и сосуды",
    "Беременность",
    "Спорт и фитнес",
    "Авторские комплексы",
    "Группа крови и резус-фактор",
    "Генетика",
]


def get_services():
    titles = []
    prices = []
    ids = []
    subcategories = []
    for i, category in enumerate(categories):
        url = f"https://lab4u.ru/saint_petersburg/store/section/{category}"
        response = requests.get(url)
        source = BeautifulSoup(response.text, "lxml")

        all_tags = source.find_all("a", {"target": "_blank"})
        tags = all_tags[: len(all_tags) - 5]
        service_hrefs = [service["href"] for service in tags]

        names = [service.text for service in tags]
        titles.extend(names)

        cost = [
            pricey.get_text(strip=True).replace("\xa0₽", "")
            for pricey in source.find_all("span", {"class": "discount-price"})
        ]
        prices.extend(cost)

        for href in service_hrefs:
            service_url = f"https://lab4u.ru/saint_petersburg{href}"
            response = requests.get(service_url)
            source = BeautifulSoup(response.text, "lxml")

            subcategories.append(cat_names[i])

            if source.find(
                    "div",
                    "complex-detail__header-pic-decor complex-detail__header-pic-decor--white",
            ):
                ids.append("")
            else:
                if (
                        "Синонимы" in source.find("p", {"class": "paragraph2"}).text
                        or "Оставьте" in source.find("p", {"class": "paragraph2"}).text
                        or "Цена" in source.find("p", {"class": "paragraph2"}).text
                ):
                    ids.append("")
                else:
                    ids.append(
                        source.find("p", {"class": "paragraph2"})
                        .text.replace("Код в номенклатуре медицинских услуг: ", "")
                        .replace("\n", "")
                        .replace(" ", "")
                        .replace("i", "")
                    )

    data = [
        [
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
            title,
            subcategories[i],
            ids[i],
            prices[i],
            "ООО «Ваша лаборатория»",
        ]
        for i, title in enumerate(titles)
    ]
    return data

    # df = pandas.DataFrame(
    #    data,
    #    columns=[
    #        "Дата",
    #        "Наименование",
    #        "Группа",
    #        "Код",
    #        "Стоимость услуги",
    #        "Наименование лаборатории",
    #    ],
    # )

    # df.to_csv("Анализы.csv")


def get_address_source():
    url = "https://lab4u.ru/medcenters/"
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("window-size=1400,800")
    driver = webdriver.Chrome(options=options)

    try:
        driver.get(url=url)
        time.sleep(3)

        select = driver.find_element(By.CLASS_NAME, "page-header2__select-wrap")
        ac = ActionChains(driver)
        ac.move_to_element(select).move_by_offset(0, 0).click().perform()
        time.sleep(2)

        spb = driver.find_element(By.XPATH, "//button[@data-city-id='4519']")
        ac.move_to_element(spb).move_by_offset(0, 0).click().perform()
        time.sleep(3)

        html = driver.find_element(
            By.CLASS_NAME, "medcentres-page__medcentre-list"
        ).get_attribute("outerHTML")
        return html
    except Exception as _ex:
        print(_ex)
    finally:
        driver.close()
        driver.quit()


def get_addresses():
    city = "Санкт-Петербург"
    phone = "8 800 555-35-90"
    addresses = []
    metro_stations = []

    source = BeautifulSoup(get_address_source(), "lxml")

    for address in source.find_all("div", {"class": "medcentres__adress"}):
        if (
                "г." not in address.text
                and "Колпино" not in address.text
                and "Ульяновка" not in address.text
        ):
            addresses.append(address.text)
            last = address.find_previous("div")
            if last["class"] == ["medcentres__subway"]:
                metro_stations.append(last.text)
            else:
                metro_stations.append("")

    data = [
        [city, addresses[i], "", phone, metro_stations[i], "ООО «Ваша лаборатория»"]
        for i in range(len(addresses))
    ]

    # df = pandas.DataFrame(
    #    data=data,
    #    columns=[
    #        "Город",
    #        "Адрес",
    #        "Часы работы",
    #        "Контактный телефон",
    #        "Станция метро",
    #    ],
    # )

    # df.to_csv("Адреса.csv")
    return data


async def add_to_db(obj, table_name):
    if table_name == 'analyzes':
        # Рассмотрим добавление данных в таблицу анализов схемы dds
        await db._insert(
            table=table_name,  # Тут название таблицы (analyzes/addresses)
            schema='stg',  # Указываем схему
            Код=obj[3],  # Здесь код анализа
            Группа=obj[2],  # Группа, к которой относится наши анализы
            Наименование=obj[1],  # Наименование нашего анализа
            Стоимость_услуги=obj[4],  # Стоимость услуги
            Наименование_лаборатории=obj[5],  # Наименование лаборатории
            Дата=obj[0],  # Дата сбора анализа
        )
    elif table_name == 'addresses':
        await db._insert(
            table=table_name,  # Тут название таблицы (analyzes/addresses)
            schema='stg',  # Указываем схему
            Город=obj[0],
            Адрес=obj[1],
            Контактные_номера=obj[3],
            Часы_работы=obj[2],
            Станция_метро=obj[4],
            Наименование_клиники=obj[5]
        )


async def parse():
    services = get_services()
    addresses = get_addresses()

    for service in services:
        await add_to_db(service, 'analyzes')

    for address in addresses:
        await add_to_db(address, 'addresses')
