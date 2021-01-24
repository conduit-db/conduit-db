import asyncpg
import logging

from .postgres_bulk_loads import PostgresBulkLoads
from .postgres_queries import PostgresQueries
from .postgres_tables import PostgresTables


class PostgresDatabase:
    """simple container for common postgres queries"""

    def __init__(self, pg_conn: asyncpg.Connection):
        self.pg_conn = pg_conn
        self.tables = PostgresTables(self.pg_conn)
        self.queries = PostgresQueries(self.pg_conn, self.tables)
        self.bulk_loads = PostgresBulkLoads(self.pg_conn)
        self.logger = logging.getLogger("pg-database")
        self.logger.setLevel(logging.DEBUG)
        # self.logger.setLevel(PROFILING)

    async def close(self):
        await self.pg_conn.close()

    # TABLES
    async def pg_drop_tables(self):
        await self.tables.pg_drop_tables()

    async def pg_drop_temp_inputs(self):
        await self.tables.pg_drop_temp_inputs()

    async def pg_drop_temp_mined_tx_hashes(self):
        await self.tables.pg_drop_temp_mined_tx_hashes()

    async def pg_drop_temp_inbound_tx_hashes(self):
        await self.tables.pg_drop_temp_inbound_tx_hashes()

    async def pg_create_permanent_tables(self):
        await self.tables.pg_create_permanent_tables()

    async def pg_create_temp_inputs_table(self):
        await self.tables.pg_create_temp_inputs_table()

    async def pg_create_temp_mined_tx_hashes_table(self):
        await self.tables.pg_create_temp_mined_tx_hashes_table()

    async def pg_create_temp_inbound_tx_hashes_table(self):
        await self.tables.pg_create_temp_inbound_tx_hashes_table()

    # QUERIES
    async def pg_load_temp_mined_tx_hashes(self, mined_tx_hashes):
        await self.queries.pg_load_temp_mined_tx_hashes(mined_tx_hashes)

    async def pg_load_temp_inbound_tx_hashes(self, inbound_tx_hashes):
        await self.queries.pg_load_temp_inbound_tx_hashes(inbound_tx_hashes)

    async def pg_get_unprocessed_txs(self, new_tx_hashes):
        return await self.queries.pg_get_unprocessed_txs(new_tx_hashes)

    async def check_for_mempool_inbound_collision(self):
        await self.queries.check_for_mempool_inbound_collision()

    async def pg_invalidate_mempool_rows(self, api_block_tip_height: int):
        await self.queries.pg_invalidate_mempool_rows(api_block_tip_height)

    async def pg_update_api_tip_height_and_hash(self, api_tip_height: int, api_tip_hash: bytes):
        await self.queries.pg_update_api_tip_height_and_hash(api_tip_height, api_tip_hash)

    # BULK LOADS
    async def pg_bulk_load_confirmed_tx_rows(self, tx_rows):
        await self.bulk_loads.pg_bulk_load_confirmed_tx_rows(tx_rows)

    async def pg_bulk_load_mempool_tx_rows(self, tx_rows):
        await self.bulk_loads.pg_bulk_load_mempool_tx_rows(tx_rows)

    async def pg_bulk_load_output_rows(self, out_rows):
        await self.bulk_loads.pg_bulk_load_output_rows(out_rows)

    async def pg_bulk_load_input_rows(self, in_rows):
        await self.bulk_loads.pg_bulk_load_input_rows(in_rows)

    async def pg_bulk_load_pushdata_rows(self, pd_rows):
        await self.bulk_loads.pg_bulk_load_pushdata_rows(pd_rows)

    async def pg_update_settings(self):
        await self.pg_conn.execute(
            """
            UPDATE pg_settings
                SET setting = 'off'
                WHERE name='synchronous_commit';
            UPDATE pg_settings
                SET setting = 200
                WHERE name='temp_buffers';
            """
        )


async def pg_connect() -> PostgresDatabase:
    conn = await asyncpg.connect(
        user="conduitadmin",
        host="127.0.0.1",
        port=5432,
        password="conduitpass",
        database="conduitdb",
    )
    return PostgresDatabase(conn)


async def pg_test_connect() -> PostgresDatabase:
    conn = await asyncpg.connect(
        user="conduitadmin",
        host="127.0.0.1",
        port=5432,
        password="conduitpass",
        database="conduittestdb",
    )
    return PostgresDatabase(conn)


async def load_pg_database() -> PostgresDatabase:
    pg_database = await pg_connect()
    await pg_database.pg_update_settings()
    await pg_database.pg_create_permanent_tables()
    return pg_database


async def load_test_pg_database() -> PostgresDatabase:
    pg_database = await pg_test_connect()
    await pg_database.pg_update_settings()
    await pg_database.pg_create_permanent_tables()
    return pg_database
