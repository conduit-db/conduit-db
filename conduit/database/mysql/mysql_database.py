import asyncio
import logging
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial

from MySQLdb import _mysql

from .mysql_bulk_loads import MySQLBulkLoads
from .mysql_queries import MySQLQueries
from .mysql_tables import MySQLTables
from ...constants import PROFILING


class MySQLDatabase:
    """simple container for common mysql queries - must maintain async/await API to match
    postgres - which then allows interop"""

    def __init__(self, mysql_conn: _mysql.connection):
        self.mysql_conn = mysql_conn
        self.tables = MySQLTables(self.mysql_conn)
        self.bulk_loads = MySQLBulkLoads(self.mysql_conn, self)
        self.queries = MySQLQueries(self.mysql_conn, self.tables, self.bulk_loads, self)

        self.executor = ThreadPoolExecutor(max_workers=1)
        self.logger = logging.getLogger("mysql-database")
        self.logger.setLevel(logging.DEBUG)
        self.logger.setLevel(PROFILING)

    async def run_in_executor(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        func = partial(func, *args, **kwargs)
        return await loop.run_in_executor(self.executor, func)

    async def close(self):
        """coroutine to maintain async/await API equivalency to postgres"""
        self.mysql_conn.close()

    def start_transaction(self):
        self.mysql_conn.query(
            """START TRANSACTION;"""
        )

    def commit_transaction(self):
        self.mysql_conn.query(
            """COMMIT;"""
        )

    # TABLES
    async def mysql_drop_tables(self):
        await self.tables.mysql_drop_tables()

    async def mysql_drop_temp_inputs(self):
        await self.tables.mysql_drop_temp_inputs()

    async def mysql_drop_temp_mined_tx_hashes(self):
        await self.tables.mysql_drop_temp_mined_tx_hashes()

    async def mysql_drop_temp_inbound_tx_hashes(self):
        await self.tables.mysql_drop_temp_inbound_tx_hashes()

    async def mysql_create_permanent_tables(self):
        await self.tables.mysql_create_permanent_tables()

    async def mysql_create_temp_inputs_table(self):
        await self.tables.mysql_create_temp_inputs_table()

    async def mysql_create_temp_mined_tx_hashes_table(self):
        await self.tables.mysql_create_temp_mined_tx_hashes_table()

    async def mysql_create_temp_inbound_tx_hashes_table(self):
        await self.tables.mysql_create_temp_inbound_tx_hashes_table()

    # QUERIES
    async def mysql_load_temp_mined_tx_hashes(self, mined_tx_hashes):
        await self.queries.mysql_load_temp_mined_tx_hashes(mined_tx_hashes)

    async def mysql_load_temp_inbound_tx_hashes(self, inbound_tx_hashes):
        await self.queries.mysql_load_temp_inbound_tx_hashes(inbound_tx_hashes)

    async def mysql_get_unprocessed_txs(self, new_tx_hashes):
        return await self.queries.mysql_get_unprocessed_txs(new_tx_hashes)

    async def check_for_mempool_inbound_collision(self):
        await self.queries.check_for_mempool_inbound_collision()

    async def mysql_invalidate_mempool_rows(self, api_block_tip_height: int):
        await self.queries.mysql_invalidate_mempool_rows(api_block_tip_height)

    async def mysql_update_api_tip_height_and_hash(self, api_tip_height: int, api_tip_hash: bytes):
        await self.queries.mysql_update_api_tip_height_and_hash(api_tip_height, api_tip_hash)

    # BULK LOADS
    async def mysql_bulk_load_confirmed_tx_rows(self, tx_rows):
        await self.bulk_loads.mysql_bulk_load_confirmed_tx_rows(tx_rows)

    async def mysql_bulk_load_mempool_tx_rows(self, tx_rows):
        await self.bulk_loads.mysql_bulk_load_mempool_tx_rows(tx_rows)

    async def mysql_bulk_load_output_rows(self, out_rows):
        await self.bulk_loads.mysql_bulk_load_output_rows(out_rows)

    async def mysql_bulk_load_input_rows(self, in_rows):
        await self.bulk_loads.mysql_bulk_load_input_rows(in_rows)

    async def mysql_bulk_load_pushdata_rows(self, pd_rows):
        await self.bulk_loads.mysql_bulk_load_pushdata_rows(pd_rows)

    async def mysql_update_settings(self):
        pass


async def mysql_connect() -> MySQLDatabase:
    """coroutine to maintain async/await API equivalency to postgres"""
    conn = _mysql.connect(
        host="127.0.0.1",
        port=3306,
        user="conduitadmin",
        passwd="conduitpass",
        db="conduitdb",
    )
    return MySQLDatabase(conn)


async def mysql_test_connect() -> MySQLDatabase:
    conn = _mysql.connect(
        host="127.0.0.1",
        port=3306,
        user="conduitadmin",
        passwd="conduitpass",
        db="conduittestdb"
    )
    return MySQLDatabase(conn)


async def load_mysql_database() -> MySQLDatabase:
    # Todo - SHOW PROCESSLIST; -> kill any sleeping connections (zombies from a previous forced
    #  kill)
    mysql_database = await mysql_connect()
    await mysql_database.mysql_update_settings()
    await mysql_database.mysql_create_permanent_tables()
    return mysql_database


async def load_test_mysql_database() -> MySQLDatabase:
    mysql_database = await mysql_test_connect()
    await mysql_database.mysql_update_settings()
    await mysql_database.mysql_create_permanent_tables()
    return mysql_database
