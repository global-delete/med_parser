from database import db
import pandas as pd
from pandas import DataFrame
import asyncio
import logging

async def get_from_db(title):
    # Рассмотрим получение данных из таблицы анализов схемы dds

    data: DataFrame = await db._get(
        table=title,
        schema='stg'
    )

    return data


async def data_processing():
    analyzes = await get_from_db('analyzes')
    analyzes['Код'] = analyzes['Код'].fillna('')
    analyzes['Группа'] = analyzes['Группа'].fillna('')

    for i in range(analyzes.shape[0]):
        if '.' in analyzes['Дата'].iloc[i]:
            analyzes['Дата'].iloc[i] = pd.to_datetime(analyzes['Дата'].iloc[i], format='%d.%m.%Y').strftime('%Y-%m-%d')

    addresses = await get_from_db('addresses')

    addresses['Станция_метро'] = addresses['Станция_метро'].fillna('')
    addresses['Часы_работы'] = addresses['Часы_работы'].fillna('')

    naz = 'г. Санкт-Петербург'
    addresses['Город'] = naz

    tel = addresses['Контактные_номера'][0]
    cleaned_sequence = ''.join(filter(str.isdigit, tel))
    num = cleaned_sequence
    num = '+7' + '(' + num[1:4] + ')' + num[4:7] + '-' + num[7:9] + '-' + num[9:11]
    addresses['Контактные_номера'] = num

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
            Стоимость_услуги=obj['Стоимость_услуги'],  # Стоимость услуги
            Наименование_лаборатории=obj['Наименование_лаборатории'],  # Наименование лаборатории
            Дата=obj['Дата'],  # Дата сбора анализа
        )
    elif table_name == 'addresses':
        await db._insert(
            table=table_name,  # Тут название таблицы (analyzes/addresses)
            schema='dds',  # Указываем схему
            Город=obj['Город'],
            Адрес=obj['Адрес'],
            Контактные_номера=obj['Контактные_номера'],
            Часы_работы=obj['Часы_работы'],
            Станция_метро=obj['Станция_метро'],
            Наименование_клиники=obj['Наименование_клиники']
        )


async def etl():
    services, addresses = await data_processing()

    for i in range(services.shape[0]):
        await add_to_db(services.iloc[i], 'analyzes')

    for i in range(addresses.shape[0]):
        await add_to_db(addresses.iloc[i], 'addresses')