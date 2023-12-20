from database import db
import pandas as pd
from pandas import DataFrame
import asyncio

async def get_from_db(title):
    # Рассмотрим получение данных из таблицы анализов схемы dds

    data: DataFrame = await db._get(
        table=title,
        schema='stg'
    )

    return data

    # Код                            123132
    # Группа                         123
    # Наименование                   Сдача крови
    # Стоимость_услуги               500 р.
    # Наименование_лаборатории       lab
    # Дата                           18.12.2023


async def data_processing():
    analyzes = await get_from_db('analyzes')
    analyzes['Код'] = analyzes['Код'].fillna('')
    analyzes['Группа'] = analyzes['Группа'].fillna('')

    analyzes['Дата'] = pd.to_datetime(analyzes['Дата']).dt.strftime('%Y-%m-%d')

    addresses = await get_from_db('addresses')

    addresses['Станция метро'] = addresses['Станция метро'].fillna('')
    addresses['Часы работы'] = addresses['Часы работы'].fillna('')

    naz = 'г. Санкт-Петербург'
    addresses['Город'] = naz

    tel = addresses['Контактный телефон'][0]
    cleaned_sequence = ''.join(filter(str.isdigit, tel))
    num = cleaned_sequence
    num = '+7' + '(' + num[1:4] + ')' + num[4:7] + '-' + num[7:9] + '-' + num[9:11]
    addresses['Контактный телефон'] = num

    return analyzes, addresses


async def add_to_db(obj, table_name):
    if table_name == 'analyzes':
        # Рассмотрим добавление данных в таблицу анализов схемы dds
        await db._insert(
            table=table_name,  # Тут название таблицы (analyzes/addresses)
            schema='dds',  # Указываем схему
            Код=obj['Код'],  # Здесь код анализа
            Группа=obj['Группа'],  # Группа, к которой относится наши анализы
            Наименование=obj['Наименование'],  # Наименование нашего анализа
            Стоимость_услуги=obj['Цена'],  # Стоимость услуги
            Наименование_лаборатории=obj['Наименование_лаборатории'],  # Наименование лаборатории
            Дата=obj['Дата'],  # Дата сбора анализа
        )
    elif table_name == 'addresses':
        await db._insert(
            table=table_name,  # Тут название таблицы (analyzes/addresses)
            schema='dds',  # Указываем схему
            Город=obj['Город'],
            Адрес=obj['Адрес'],
            Контактные_номера=obj['Контактные_данные'],
            Часы_работы=obj['Часы_работы'],
            Станция_метро=obj['Станция_метро'],
            Наименование_клиники=obj['Наименование_клиники']
        )


async def etl():
    services, addresses = data_processing()

    for service in services:
        await add_to_db(service, 'analyzes')

    for address in addresses:
        await add_to_db(address, 'addresses')