import asyncio
import logging

from KrilovMisha.main import parser as parser1
from Lyusin_Dmitry.Parsing import address_data as parser2_adresses, analysis_data as parser2_analyzes 
from BulygaEkaterina.Parser import parse as parser3

async def parsing_1():
    logging.warning("Parser1 - is running")
    try:
        await parser1.parse()
    except Exception as err:
        logging.critical(f"Parser1 - error: {err}")
    else:
        logging.warning("Parser1 - Done!")
    asyncio.get_event_loop().call_later(60*60*12, asyncio.ensure_future, parsing_1())

async def parsing_2():
    logging.warning("Parser2 - is running")
    try:
        parser2_adresses()
        parser2_analyzes()
    except Exception as err:
        logging.critical(f"Parser2 - error: {err}")
    else:
        logging.warning("Parser2 - Done!")
    asyncio.get_event_loop().call_later(60*60*12, asyncio.ensure_future, parsing_2())

async def parsing_3():
    logging.warning("Parser3 - is running")
    try:
        await parser3()
    except Exception as err:
        logging.critical(f"Parser3 - error: {err}")
    else:
        logging.warning("Parser3 - Done!")
    asyncio.get_event_loop().call_later(60*60*12, asyncio.ensure_future, parsing_3())

async def parsing_4():
    logging.warning("Parser4 - is running")
    try:
        ...
        # await parser4()
    except Exception as err:
        logging.critical(f"Parser4 - error: {err}")
    else:
        logging.warning("Parser4 - Done!")
    asyncio.get_event_loop().call_later(60*60*12, asyncio.ensure_future, parsing_4())

# asyncio.ensure_future(parsing_1())
# asyncio.ensure_future(parsing_2())
# asyncio.ensure_future(parsing_3())

loop = asyncio.get_event_loop()
loop.run_forever()
