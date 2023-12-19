import asyncio
import logging

from KrilovMisha.main import parser as parser1
from Lyusin_Dmitry.Parsing import address_data as parser2_adresses, analysis_data as parser2_analyzes 
from BulygaEkaterina.Parser import parse as parser3

async def parsing_1():
    parser1.parse()
    asyncio.get_event_loop().call_later(60*60*12, asyncio.ensure_future, parsing_1())

async def parsing_2():
    parser2_adresses()
    parser2_analyzes()
    asyncio.get_event_loop().call_later(60*60*12, asyncio.ensure_future, parsing_2())

async def parsing_3():
    await parser3()
    asyncio.get_event_loop().call_later(60*60*12, asyncio.ensure_future, parsing_3())

# Запуск асинхронной задачи
asyncio.ensure_future(parsing_1())
asyncio.ensure_future(parsing_2())
asyncio.ensure_future(parsing_3())
loop = asyncio.get_event_loop()
loop.run_forever()
