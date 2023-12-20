import asyncio
import logging

from KrilovMisha.main import parser as parser1
from Lyusin_Dmitry.Parsing import parse as parser2
from BulygaEkaterina.Parser import parse as parser3
from ShevchenkoSemyon.parser import parse as parser4
from TikhonovaMarina.lab4u import parse as parser5
from Lyusin_Dmitry.ETL import etl


async def parsing_1():

    logging.warning("Parser1 - is running")
    try:
        await parser1.parse()
    except Exception as err:
        logging.critical(f"Parser1 - error: {err}")
    else:
        logging.warning("Parser1 - Done!")
    await parsing_2()
    asyncio.get_event_loop().call_later(
        60 * 60 * 12, asyncio.ensure_future, parsing_1()
    )

async def parsing_2():
    logging.warning("Parser2 - is running")
    try:
        await parser2()
    except Exception as err:
        logging.critical(f"Parser2 - error: {err}")
    else:
        logging.warning("Parser2 - Done!")

    await parsing_3()


async def parsing_3():
    logging.warning("Parser3 - is running")
    try:
        await parser3()
    except Exception as err:
        logging.critical(f"Parser3 - error: {err}")
    else:
        logging.warning("Parser3 - Done!")

    await parsing_4()


async def parsing_4():
    logging.warning("Parser4 - is running")
    try:
        await parser4()
    except Exception as err:
        logging.critical(f"Parser4 - error: {err}")
    else:
        logging.warning("Parser4 - Done!")

    await etl_process()

    # await parsing_5()


# async def parsing_5():
#     logging.warning("Parser5 - is running")
#     await parser5()
#     try:
#         # await parser5()
#         ...
#     except Exception as err:
#         logging.critical(f"Parser5 - error: {err}")
#     else:
#         logging.warning("Parser5 - Done!")


async def etl_process():
    logging.warning("etl - is running")
    try:
        await etl()
    except Exception as err:
        logging.critical(f"etl - error: {err}")
    else:
        logging.warning("etl - Done!")


asyncio.ensure_future(parsing_1())

loop = asyncio.get_event_loop()
loop.run_forever()
