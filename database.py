import asyncpg
import asyncio
import os
from pandas import DataFrame

from typing import Generator, Coroutine, Any, List

if os.name == 'nt':
    cnfg = {
        "user": "postgres",
        "password": "postgres",
        "database": "postgres",
        "host": "localhost",
        "port": "6543",
    }
else:
    cnfg = {
        "user": "postgres",
        "password": "postgres",
        "database": "postgres",
        "host": "postgres",
        "port": "5432",
    }    


class Connection:
    async def __aenter__(self) -> asyncpg.connection.Connection:
        self.connection = await self._connect()
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.connection.close()

    @staticmethod
    async def _connect() -> asyncpg.connection.Connection:
        return await asyncpg.connect(
            user=cnfg["user"],
            password=cnfg["password"],
            database=cnfg["database"],
            host=cnfg["host"],
            port=cnfg["port"],
            timeout=600
        )

class DataBase:
    __tables: List[str] = ["analyzes", "addresses"]
    __schemes: List[str] = ['STG', 'DDS']
    __columns: dict = {
        "analyzes": [
            ["Код", "text"],
            ["Группа", "text"],
            ["Наименование", "text"],
            ["Стоимость_услуги", "text"],
            ["Наименование_лаборатории", "text"],
            ["Дата", "text"],
        ],
        "addresses": [
            ["Город", "text"],
            ["Адрес", "text"],
            ["Контактные_номера", "text"],
            ["Часы_работы", "text"],
            ["Станция_метро", "text"],
            ["Наименование_клиники", "text"],
        ],
    }

    def __init__(self) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.create_schemes())
        loop.run_until_complete(self.create_tables())

    async def _insert(self, table, schema, **kwargs) -> Coroutine:
        async with Connection() as connection:
            f = f"INSERT INTO {schema}.{table} ({', '.join([str(i) for i in kwargs])}) VALUES ({', '.join([f'${index+1}' for index, element in enumerate(kwargs)])});"
            values = [kwargs[key] for key in kwargs]
            return await connection.fetchval(f, *values)

    async def _get(self, table, schema) -> Coroutine:
        async with Connection() as connection:
            result = {}
            for i in self.__columns[table]:
                result[i[0]] = []
            for i in await connection.fetch(f"SELECT * FROM {schema}.{table}"):
                for j in dict(i):
                    result[j].append(i[j])
            if bool(await connection.fetch(f'SELECT 1 FROM {schema}.{table}')):
                return DataFrame.from_dict(
                    data=result, orient='index'
                )
            return DataFrame(data=None, columns=list(result))

    async def check_tables(self) -> Generator:
        connection: asyncpg.connection.Connection = await Connection._connect()

        for schema in self.__schemes:
            for table in self.__tables:
                result: bool = await connection.fetchval(
                    f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table.lower()}' AND table_schema = '{schema.lower()}');"
                )

                yield result, table, schema

        await connection.close()

    async def check_schemes(self) -> Generator:
        connection: asyncpg.connection.Connection = await Connection._connect()

        for schema in self.__schemes:
            result: bool = await connection.fetchval(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema.lower()}');"
            )

            yield result, schema

        await connection.close()   

    async def create_schemes(self) -> None:
        schemes = [i[1] async for i in self.check_schemes() if not i[0]]

        connection: asyncpg.connection.Connection = await Connection._connect()

        for schema in schemes:
            await connection.fetchval(
                f"CREATE SCHEMA {schema};"
            )

        await connection.close()

    async def create_tables(self) -> None:
        tables = [(i[1], i[2]) async for i in self.check_tables() if not i[0]]

        connection: asyncpg.connection.Connection = await Connection._connect()

        for table, schema in tables:
            await connection.fetchval(
                f"CREATE TABLE {schema}.{table}({', '.join([' '.join(i) for i in self.__columns[table]])});"
            )

        await connection.close()


db = DataBase()