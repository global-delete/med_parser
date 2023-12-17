import asyncpg
import asyncio

from typing import Generator, Coroutine, Any, List

cnfg = {
    'user': 'airflow',
    'password': 'airflow',
    'database': 'postgres',
    'host': '0.0.0.0',
    'port': '5432'
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
            port=cnfg['port']
        )

class DataBase:
    __tables: List[str] = ['analyzes', 'addresses']
    __columns: dict = {
        'analyzes': [
            ['Код', 'text'],
            ['Группа', 'text'],
            ['Наименование', 'text'],
            ['Стоимость_услуги', 'text'],
            ['Наименование_лаборатории', 'text'],
            ['Дата', 'text']
        ],
        'addresses': [
            ['Город', 'text'],
            ['Адрес', 'text'],
            ['Контактные_номера', 'text'],
            ['Часы_работы', 'text'],
            ['Станция_метро', 'text']
        ]
    }

    def __init__(self) -> None:
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self.create_tables())

    async def _insert(self, table, **kwargs) -> Coroutine:
        async with Connection() as connection:
            f = f"INSERT INTO {table} ({', '.join([str(i) for i in kwargs])}) VALUES ({', '.join([f'${index+1}' for index, element in enumerate(kwargs)])});"
            values = [kwargs[key] for key in kwargs]
            return await connection.fetchval(f, *values)

    async def check_tables(self) -> Generator:
        connection: asyncpg.connection.Connection = await Connection._connect()

        for table in self.__tables:
            result: bool = await connection.fetchval(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table.lower()}');"
            )
            
            yield result, table

        await connection.close()

    async def create_tables(self, tables: List[str] = []) -> None:
        if not tables:
            tables = [i[1] async for i in self.check_tables() if not i[0]]

        connection: asyncpg.connection.Connection = await Connection._connect()
        
        for table in tables:
            await connection.fetchval(
                f"create table {table}({', '.join([' '.join(i) for i in self.__columns[table]])});"
            )

        await connection.close()

db = DataBase()